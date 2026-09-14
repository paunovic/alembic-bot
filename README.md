# alembic-bot

two small bots that keep alembic migration histories single-headed

## alembic_bot_pr.py

runs against github: it lists open pull requests, finds migrations
whose down_revision no longer points at master's head, and fixes
them - merges master into the PR branch, repoints the revision
(and renumbers sequential ids), and pushes.

    GITHUB_TOKEN=... GITHUB_REPOSITORY=org/repo python alembic_bot_pr.py

meant for a scheduled action. needs `requests` and a checkout of the
repository.

## alembic_bot_gocd.py

runs on master (gocd in its original home, any ci will do): it walks
every alembic tree in the repository, and when a merge has left two
heads behind it writes a merge-heads migration that collapses them,
commits, and posts a slack note about it. a dynamodb lock keeps two
instances from racing; master being re-pulled mid-run restarts the
bot instead of pushing over someone.

    GITHUB_REPOSITORY=org/repo GOCD_SLACK_OAUTH_TOKEN=... \
    GOCD_SLACK_CHANNEL=C123,C456 python alembic_bot_gocd.py

needs `requests` and `botocore`; aws credentials resolve through the
ambient chain.
