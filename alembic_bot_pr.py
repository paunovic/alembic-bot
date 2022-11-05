import ast
import os
import re
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


REPOSITORY: str = "organization/code.organization.com"
GITHUB_TOKEN: str = os.environ["GITHUB_TOKEN"]

master_commits: List[str] = []


def get_github_open_pull_requests() -> List[dict]:
    data = []
    page = 1

    while True:
        response = get(f"https://api.github.com/repos/{REPOSITORY}/pulls?state=open&sort=created&per_page=100&page={page}")
        page_data = response.json()
        data.extend(page_data)
        if len(page_data) < 100:
            break
        page += 1

    return data


def get_github_pull_request_changed_files(pr_number: int) -> List[dict]:
    data = []
    page = 1

    while True:
        response = get(f"https://api.github.com/repos/{REPOSITORY}/pulls/{pr_number}/files?per_page=100&page={page}")
        page_data = response.json()
        data.extend(page_data)
        if len(page_data) < 100:
            break
        page += 1

    return data


def get_github_pull_request_info(pr_number: int) -> dict:
    response = get(f"https://api.github.com/repos/{REPOSITORY}/pulls/{pr_number}")
    return response.json()


def get_github_file_contents(sha: str, path: str) -> bytes:
    response = get(f"https://raw.githubusercontent.com/{REPOSITORY}/{sha}/{path}")
    return response.content


def get_next_100_github_master_commits() -> None:
    page = len(master_commits) // 100
    response = get(f"https://api.github.com/repos/{REPOSITORY}/commits?page={page}&per_page=100")
    for commit in response.json():
        master_commits.append(commit["sha"])


def get_alembic_ini_paths() -> list:
    # these directories should never be traversed into
    ignored_directories = [
        "__pycache__",
        "node_modules",
        ".git",
        ".pnpm-store",
    ]

    alembic_paths = []

    for dirpath, dirnames, _ in os.walk("."):
        for ignored_name in ignored_directories:
            if ignored_name in dirnames:
                dirnames.remove(ignored_name)

        for dirname in dirnames:
            alembic_file = os.path.join(dirpath, dirname, "alembic.ini")
            if os.path.isfile(alembic_file):
                alembic_paths.append(alembic_file)

    return alembic_paths


def get_alembic_revision_map() -> Dict[str, Dict[str, Any]]:

    script_location_regex = re.compile(r"^script_location = (\w+)$", re.DOTALL | re.MULTILINE)

    alembic_map = {}
    for alembic_ini_path in get_alembic_ini_paths():
        with open(alembic_ini_path, "r") as fp:
            alembic_data = fp.read()

        script_locations = script_location_regex.findall(alembic_data)

        for script_location in script_locations:
            script_location = os.path.join(os.path.dirname(alembic_ini_path), script_location)
            script_location = os.path.abspath(script_location)
            version_location = os.path.join(script_location, "versions")

            revision_history = set()
            down_revisions = set()

            for dirpath, dirnames, filenames in os.walk(version_location):
                for filename in filenames:
                    if not filename.endswith(".py"):
                        continue

                    filepath = os.path.join(dirpath, filename)
                    with open(filepath, "rb") as fp:
                        file_contents = fp.read()

                    revision, down_revision = parse_revisions_from_file(file_contents)
                    if not revision and not down_revision:
                        continue

                    revision_history.add(revision)

                    if down_revision is None:
                        pass
                    elif isinstance(down_revision, str):
                        down_revisions.add(down_revision)
                    elif isinstance(down_revision, tuple):
                        down_revisions.update(down_revision)
                    else:
                        raise Exception(f"Implementation Error - unknown down_revision type: {down_revision}")

            if len(revision_history) == 0 and len(down_revisions) == 0:
                print(f"no revisions found for {script_location}")
                continue

            head_revision = revision_history - down_revisions
            assert len(head_revision) == 1, (script_location, head_revision)
            head_revision = head_revision.pop()

            alembic_map[script_location] = {
                "head": head_revision,
                "history": revision_history,
            }

    return alembic_map


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
        revision_match = re.search(rb"revision = ['\"]([\w_]+)['\"]", file_contents)
        down_revision_match = re.search(rb"down_revision = ['\"]([\w_]+)['\"]", file_contents)
        if revision_match and down_revision_match:
            revision = revision_match.group(1)
            down_revision = down_revision_match.group(1)

    return revision, down_revision


def execute(*args) -> int:
    print(*args)

    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if stdout:
        print(stdout.decode("utf-8"))
    if stderr:
        print(stderr.decode("utf-8"))

    return process.returncode


def update_pull_request(pr_number: int, files_to_update: List[dict]) -> None:
    pr_info: dict = get_github_pull_request_info(pr_number)
    pr_sha: str = pr_info["head"]["sha"]
    pr_branch: str = pr_info["head"]["ref"]
    base_sha: str = pr_info["base"]["sha"]

    fetches = 0
    while base_sha not in master_commits:
        fetches += 1
        if fetches >= 50:
            print(f"`git fetch master` reached maximum fetch depth of {fetches}")
            return

        # fetch 99 commits first time, as there is already one commit fetched by GitHub Action
        deepen = 100 if master_commits else 99
        if return_code := execute("git", "fetch", f"--deepen={deepen}", "origin", "master"):
            print(f"`git fetch master` failed with return code {return_code}")
            return

        get_next_100_github_master_commits()

    if return_code := execute("git", "fetch", "origin", pr_branch):
        print(f"`git fetch {pr_branch}` failed with return code {return_code}")
        return

    if return_code := execute("git", "checkout", "-b", pr_branch, f"origin/{pr_branch}"):
        print(f"`git checkout` failed with return code {return_code}")
        return

    commit_messages: List[str] = get_last_commit_messages(6)
    if not commit_messages:
        print("failed to retrieve commit messages, skipping update")
        return

    # dont update stale PRs to save some $$$
    commit_messages = list(filter(lambda m: not m.startswith("Merge branch 'master'"), commit_messages))
    if commit_messages[:3] == [
        "update alembic revision id",
        "update alembic revision id",
        "update alembic revision id",
    ]:
        print("not updating revision id, already updated three times")
        return

    if return_code := execute("git", "merge", "master"):
        print(f"`git merge` failed with return code {return_code}")
        execute("git", "merge", "--abort")

    print(f"fixing alembic revisions for branch {pr_branch}")
    for file_to_update in files_to_update:
        filename: str = file_to_update["filename"]
        file_contents: bytes = get_github_file_contents(pr_sha, filename)

        # update revision id
        new_file_contents: bytes = re.sub(
            file_to_update["down_revision"].encode("utf-8"),
            file_to_update["head_revision"].encode("utf-8"),
            file_contents,
        )

        # if revision ids are sequential, bump the filename and current revision id by one
        if match := re.search(r"^(\d{5})_(.{5})$", file_to_update["head_revision"]):
            revision_match = re.search(r"^(\d{5})_(.{5})$", file_to_update["revision"])
            new_revision_id: str = (
                str((int(match.group(1)) + 1)).zfill(5)
                + "_"
                + (revision_match or match).group(2)
            )

            os.unlink(filename)
            execute("git", "rm", filename)

            new_filename: str = filename.replace(file_to_update["revision"], new_revision_id)
            new_file_contents = re.sub(
                file_to_update["revision"].encode("utf-8"),
                new_revision_id.encode("utf-8"),
                new_file_contents,
            )
            with open(new_filename, "wb") as fp:
                fp.write(new_file_contents)
                fp.truncate()
            execute("git", "add", new_filename)
        else:
            with open(filename, "wb") as fp:
                fp.write(new_file_contents)
                fp.truncate()

    if return_code := execute("git", "commit", "-am", "update alembic revision id"):
        print(f"`git commit` failed with return code {return_code}")
        return

    if return_code := execute("git", "push"):
        print(f"`git push` failed with return code {return_code}")
        return

    print(f"alembic revision fix pushed to branch {pr_branch}")


def get_last_commit_messages(n: int) -> List[str]:
    process = subprocess.Popen(
        ["git", "log", "-n", str(n), "--format='%s'"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if process.returncode:
        print(f"`git log` failed with return code {process.returncode}")
        if stdout:
            print(stdout.decode("utf-8"))
        if stderr:
            print(stderr.decode("utf-8"))
        return []

    messages: List[str] = []
    for line in stdout.decode("utf-8").split("\n"):
        line = line.strip(" '")
        if line:
            messages.append(line)

    return messages


def get(url: str) -> requests.Response:
    for attempt in range(5):
        try:
            response = requests.get(url=url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError:
            if attempt == range(5)[-1]:
                raise
            time.sleep(attempt + 1)


def fix_alembic_revisions() -> None:

    alembic_revisions = get_alembic_revision_map()

    for open_pr in get_github_open_pull_requests():
        pr_sha = open_pr["head"]["sha"]
        pr_number = open_pr["number"]
        pr_branch = open_pr["head"]["ref"]
        base_branch = open_pr["base"]["ref"]

        if base_branch != "master":
            continue

        print(f"checking pull request #{pr_number}: {pr_branch}")

        files_to_update = []

        # get list of changed files for the pull request
        changed_files = get_github_pull_request_changed_files(pr_number)

        for changed_file in changed_files:
            filename = changed_file["filename"]
            abs_filename = os.path.abspath(filename)

            # skip removed files
            if changed_file["status"] == "removed":
                continue

            # skip non-python files
            if not filename.endswith(".py"):
                continue

            # skip files that are not in the alembic directory
            for script_location in alembic_revisions:
                if abs_filename.startswith(script_location):
                    alembic_script_location = script_location
                    break
            else:
                continue

            head_revision = alembic_revisions[alembic_script_location]["head"]
            revision_history = alembic_revisions[alembic_script_location]["history"]

            # if revision history is empty, do nothing
            if not revision_history:
                continue

            # get full file contents
            file_contents: bytes = get_github_file_contents(pr_sha, filename)

            # attempt to parse revision and down_revision from the file
            revision, down_revision = parse_revisions_from_file(file_contents)
            if not revision or not down_revision:
                continue

            # if revision already exists in the revision history then
            # existing migration is probably being edited in-place
            # do nothing in this case
            if revision in revision_history:
                continue

            # if down_revision does not exist in the revision history
            # then the PR probably contains multiple alembic migrations and
            # this file comes later in the PR's migration chain
            if down_revision not in revision_history:
                continue

            # if down_revision matches head revision, all is good
            if down_revision == head_revision:
                continue

            print(
                "down_revision {}/{} -> {} in {} does not match head revision in master: {}".format(
                    os.path.basename(alembic_script_location),
                    down_revision,
                    revision,
                    pr_branch,
                    head_revision,
                )
            )

            files_to_update.append({
                "filename": filename,
                "revision": revision,
                "down_revision": down_revision,
                "head_revision": head_revision,
            })

        # if there are files to update, merge master and push commit with
        # fixed revision ids to the PR
        if files_to_update:
            update_pull_request(pr_number, files_to_update)


if __name__ == "__main__":
    fix_alembic_revisions()
