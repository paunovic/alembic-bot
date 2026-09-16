# alembic-bot

two small bots that keep alembic migration histories single-headed

## alembic_bot_pr.py

runs against github: it lists open pull requests, finds migrations
whose down_revision no longer points at master's head, and fixes
them - merges master into the PR branch, repoints the revision
(and renumbers sequential ids), and pushes.

## alembic_bot_gocd.py

runs on master in CI/CD: walks every alembic tree in the repository,
and when a merge has left two heads behind, it writes a merge-heads
migration that collapses them, commits, and posts a slack note about it.
a dynamodb lock keeps two bot instances from causing race condition.
