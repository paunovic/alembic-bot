# alembic-bot

Two small bots that keep alembic migration histories single-headed.

## alembic_bot_pr.py

Runs against github: it lists open pull requests, finds migrations
whose down_revision no longer points at master's head, and fixes
them - merges master into the PR branch, repoints the revision
(and renumbers sequential ids), and pushes.

## alembic_bot_gocd.py

Runs on master, in gocd or any other ci/cd: walks every alembic
tree in the repository, and when a merge has left two heads behind,
writes a merge-heads migration that collapses them, commits, and posts
a slack note about it. A dynamodb lock keeps two bot instances from
causing race condition.
