import ast
import datetime
import os
import re
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from http import HTTPStatus
from typing import Generator, Optional, Tuple

import botocore.exceptions
import botocore.session
import requests


dynamodb_config = {}  # XXX: credentials to authenticate to AWS
bs = botocore.session.get_session()
dynamodb_client = bs.create_client(
    service_name="dynamodb",
    **dynamodb_config,
)


slack_messages = []


def get_current_git_head() -> Optional[str]:
    process = subprocess.Popen(
        ["git", "rev-parse", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if process.returncode:
        print("`git rev-parse HEAD` failed with return code {}, stdout: {}; stderr: {}".format(
            process.returncode,
            stdout.decode("utf-8").rstrip(" \n"),
            stderr.decode("utf-8").rstrip(" \n"),
        ))
        return None

    return stdout.decode("utf-8").rstrip(" \n")


def execute(*args) -> int:
    print("$", *args)

    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if stdout:
        print(stdout.decode("utf-8").rstrip(" \n"))
    if stderr:
        print(stderr.decode("utf-8").rstrip(" \n"))

    return process.returncode


def parse_revisions_from_file(file_contents: bytes) -> Tuple[Optional[str], Optional[str]]:
    revision = None
    down_revision = None

    try:
        for node in ast.parse(file_contents.decode("utf-8")).body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if target.id == "revision":
                        revision = node.value.value
                    elif target.id == "down_revision":
                        down_revision = node.value
                        if isinstance(down_revision, ast.Constant):
                            down_revision = down_revision.value
                        elif isinstance(down_revision, ast.Tuple):
                            down_revision = tuple(d.value for d in down_revision.elts)
    except Exception:
        print(f"failed to parse file contents with AST: {file_contents}")
        revision_match = re.search(rb"revision = ['\"]([\w_]+)['\"]", file_contents)
        down_revision_match = re.search(rb"down_revision = ['\"]([\w_]+)['\"]", file_contents)
        if revision_match and down_revision_match:
            revision = revision_match.group(1)
            down_revision = down_revision_match.group(1)

    return revision, down_revision


def yield_alembic_ini_paths() -> Generator[str, None, None]:
    # these directories should never be traversed into
    ignored_directories = [
        "__pycache__",
        "node_modules",
        ".git",
        ".pnpm-store",
    ]

    for dirpath, dirnames, _ in os.walk("."):
        for ignored_name in ignored_directories:
            if ignored_name in dirnames:
                dirnames.remove(ignored_name)

        for dirname in dirnames:
            alembic_file = os.path.join(dirpath, dirname, "alembic.ini")
            if os.path.isfile(alembic_file):
                yield alembic_file


def yield_alembic_migrations_directories() -> Generator[str, None, None]:
    script_location_regex = re.compile(r"^script_location = (\w+)$", re.DOTALL | re.MULTILINE)

    for alembic_ini_path in yield_alembic_ini_paths():
        with open(alembic_ini_path, "r") as fp:
            alembic_data = fp.read()

        migrations_dirs = set()
        script_locations = script_location_regex.findall(alembic_data)
        for script_location in script_locations:
            script_location = os.path.join(os.path.dirname(alembic_ini_path), script_location)
            script_location = os.path.abspath(script_location)
            migrations_dir = os.path.join(script_location, "versions")
            migrations_dirs.add(migrations_dir)

        for migrations_dir in migrations_dirs:
            yield migrations_dir


def get_alembic_revision_graph(migrations_dir: str) -> Tuple[dict, str, dict]:
    revision_map = dict()
    graph = dict()

    origin_revision = None
    for dirpath, dirnames, filenames in os.walk(migrations_dir):
        for filename in filenames:
            if not filename.endswith(".py"):
                continue

            filepath = os.path.join(dirpath, filename)
            with open(filepath, "rb") as fp:
                file_contents = fp.read()

            revision, down_revision = parse_revisions_from_file(file_contents)
            if not revision and not down_revision:
                continue

            if down_revision is None:
                if origin_revision:
                    raise Exception(f"found multiple origin revisions: {origin_revision}, {revision}")
                origin_revision = revision
                down_revision = ()
            elif isinstance(down_revision, str):
                down_revision = (down_revision,)

            revision_map[revision] = {
                "revision": revision,
                "parents": (),
                "children": down_revision,
                "filepath": filepath,
            }

    if not origin_revision:
        raise Exception("no origin revision found")

    graph[origin_revision] = revision_map[origin_revision]

    def build_graph(node: dict) -> None:
        for rev, revnode in revision_map.items():
            if node["revision"] in revnode["children"]:
                node["parents"] += (rev,)
                if revnode["revision"] not in graph:
                    graph[revnode["revision"]] = revnode.copy()
                    build_graph(graph[revnode["revision"]])

    build_graph(graph[origin_revision])

    return graph, origin_revision, revision_map


def find_heads(graph: dict, node: dict) -> list:
    heads = []
    processed_revisions = set()

    revisions = set(node["parents"]) or {node["revision"]}
    while revisions:
        revision = revisions.pop()

        # if revision has no parents, it's a head revision
        if not graph[revision]["parents"]:
            heads.append(graph[revision])
            processed_revisions.add(revision)
            continue

        for parent_rev in graph[revision]["parents"]:
            if parent_rev not in processed_revisions:
                revisions.add(parent_rev)
                processed_revisions.add(parent_rev)

    return heads


def insert_node(graph: dict, revision_map: dict, revision: str, children: tuple, parents: tuple, filepath: str) -> None:
    revision_map[revision] = {
        "revision": revision,
        "parents": parents,
        "children": children,
        "filepath": filepath,
    }

    graph[revision] = revision_map[revision].copy()

    for child in children:
        if child not in graph:
            raise Exception(f"Child revision {child} not found in graph")
        graph[child]["parents"] += (revision,)

    for parent_rev in parents:
        if parent_rev not in graph:
            raise Exception(f"Parent revision {parent_rev} not found in graph")
        graph[parent_rev]["children"] += (revision,)


def remove_node(graph: dict, revision_map: dict, revision: str) -> None:
    for child in graph[revision]["children"]:
        graph[child]["parents"] = tuple(parent_rev for parent_rev in graph[child]["parents"] if parent_rev != revision)

    for parent_rev in graph[revision]["parents"]:
        graph[parent_rev]["children"] = tuple(child for child in graph[parent_rev]["children"] if child != revision)

    del graph[revision]

    del revision_map[revision]
    for revnode in revision_map.values():
        revnode["children"] = tuple(child for child in revnode["children"] if child != revision)
        revnode["parents"] = tuple(parent_rev for parent_rev in revnode["parents"] if parent_rev != revision)


def alembic_name(path: str) -> str:
    # return alembic folder name (i.e. `alembic_adjudication`) from path
    return re.search(r"(alembic_\w+)", path).group(1)


def git_path(path: str) -> str:
    # determine git path from file system path
    # XXX: fixme
    return path


def merge_heads(graph: dict, revision_map: dict, migrations_dir: str, revisions: list) -> str:
    print(f"merging heads: {revisions}")

    merge_head_revision = format(uuid.uuid4().hex[:12])

    # for sequential revisions, bump merge head revision by one
    max_rev_count = 0
    for revision in revisions:
        match = re.search(r"^(\d{5})_(.{5})$", revision)
        if not match:
            break
        rev_count = int(match.group(1))
        if rev_count > max_rev_count:
            max_rev_count = rev_count
    else:
        merge_head_revision = "{:05d}_{}".format(max_rev_count + 1, uuid.uuid4().hex[:5])

    merge_heads_file_contents: str = """\"\"\"merge heads

Revision ID: {}
Revises: {}
Create Date: {}

\"\"\"

revision = \"{}\"
down_revision = ({})
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
""".format(
        merge_head_revision,
        ", ".join(revisions),
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        merge_head_revision,
        ", ".join(f'"{rev}"' for rev in revisions),
    )

    merge_heads_file = os.path.join(migrations_dir, f"{merge_head_revision}_merge_heads.py")
    with open(merge_heads_file, "w") as fp:
        fp.write(merge_heads_file_contents)

    execute("git", "add", merge_heads_file)

    insert_node(
        graph=graph,
        revision_map=revision_map,
        revision=merge_head_revision,
        children=tuple(revisions),
        parents=(),
        filepath=merge_heads_file,
    )

    return merge_head_revision


def send_slack_notifications() -> None:
    if not slack_messages:
        print("*** WARNING: nothing to notify slack about")
        return

    if not (slack_oauth_token := os.environ.get("GOCD_SLACK_OAUTH_TOKEN")):
        print("*** WARNING: GOCD_SLACK_OAUTH_TOKEN not set, skipping slack notification ***")
        return

    job_name = os.environ["GO_JOB_NAME"]

    job_url = "{}/go/tab/build/detail/{}/{}/{}/{}/{}".format(
        os.environ["GOCD_URL"],
        os.environ["GO_PIPELINE_NAME"],
        os.environ["GO_PIPELINE_COUNTER"],
        os.environ["GO_STAGE_NAME"],
        os.environ["GO_STAGE_COUNTER"],
        job_name,
    )

    pretext_parts = [
        f"Job: <{job_url}|{job_name}>",
    ]

    if git_head := get_current_git_head():
        pretext_parts.append("Commit: <{}|{}>".format(
            f"https://github.com/organization/code.organization.com/commit/{git_head}",  # XXX: update github url
            git_head[:10],
        ))

    pretext = ":alembic: :hammer_and_wrench: *{}*".format(
        "  ".join(pretext_parts),
    )

    message = "\n".join(slack_messages)
    message = f"```{message}```"
    message = message.rstrip("\n")[:3000]  # slack api message length has 3000 char limit

    gocd_slack_channels = os.environ.get("GOCD_SLACK_CHANNEL", "")
    gocd_slack_channel_ids = {c.strip() for c in gocd_slack_channels.split(",") if c}
    gocd_slack_channel_ids.add("C033T2W8SBX")  # always send notifications to #gocd-notifications channel

    for slack_channel_id in gocd_slack_channel_ids:
        response = requests.post(
            url="https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {slack_oauth_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={
                "channel": slack_channel_id,
                "as_user": True,
                "attachments": [
                    {
                        "pretext": pretext,
                        "color": "warning",
                        "text": message,
                    },
                ],
            },
        )
        if response.status_code != HTTPStatus.OK or not response.json()["ok"]:
            print(f"failed to send notification to {slack_channel_id} ({response.status_code=}: {response.content=!r})")
        else:
            print(f"notified channel {slack_channel_id}")


def main() -> None:
    # pull once to ensure we are up to date
    if return_code := execute("git", "pull"):
        print(f"`git pull` failed with return code {return_code}")
        sys.exit(return_code)

    slack_messages.clear()
    commit_messages = []

    for migrations_dir in yield_alembic_migrations_directories():
        print(f"processing migrations: {migrations_dir}")

        # get revision graph
        graph, origin_revision, revision_map = get_alembic_revision_graph(migrations_dir)

        # find heads
        heads = find_heads(graph, graph[origin_revision])

        # merge heads
        if len(heads) > 1:
            revisions_to_merge = [rev["revision"] for rev in heads]
            merge_head_revision = merge_heads(graph, revision_map, migrations_dir, revisions_to_merge)

            message = "merge heads ({}): {} -> {}".format(
                alembic_name(migrations_dir),
                ", ".join(revisions_to_merge),
                merge_head_revision,
            )
            print(message)
            commit_messages.append(message)
            slack_messages.append(message)

    # commit to github
    if commit_messages:
        # git pull to ensure there are no new changes in meantime
        # if there are new changes, restart bot run and don't push to avoid race conditions with other jobs
        process = subprocess.Popen(
            ["git", "pull"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate()
        if stdout:
            print(stdout.decode("utf-8").rstrip(" \n"))
            if "Already up to date." not in stdout.decode("utf-8"):
                print("new changes detected in master, restarting bot run to avoid race conditions")
                # remove untracked files
                if return_code := execute("git", "clean", "-fd"):
                    print(f"`git clean -fd` failed with return code {return_code}")
                    sys.exit(return_code)
                return main()

        if stderr:
            print(stderr.decode("utf-8").rstrip(" \n"))

        if process.returncode:
            print(f"`git pull` failed with return code {process.returncode}")
            sys.exit(process.returncode)

        if return_code := execute("git", "commit", "-am", "\n".join(commit_messages)):
            print(f"`git commit` failed with return code {return_code}")
            sys.exit(return_code)

        if return_code := execute("git", "push"):
            print(f"`git push` failed with return code {return_code}")
            sys.exit(return_code)

        send_slack_notifications()

    print("done.")


@contextmanager
def lock() -> Generator[None, None, None]:
    # use dynamodb as a lock to ensure only one instance of this bot is running at a time
    wait_count = 0
    while True:
        try:
            dynamodb_client.put_item(
                TableName="alembic_bot",
                Item={
                    "id": {"S": "lock"},
                },
                ConditionExpression="attribute_not_exists(id)",
            )
            break
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                wait_count += 1
                if wait_count == 90:
                    print("another bot still running after 15 minutes, aborting")
                    sys.exit(1)

                print("another bot already running, waiting...")
                time.sleep(10)
                continue

            raise

    try:
        yield
    except Exception:
        raise
    finally:
        dynamodb_client.delete_item(
            TableName="alembic_bot",
            Key={
                "id": {"S": "lock"},
            },
        )


if __name__ == "__main__":
    with lock():
        main()
