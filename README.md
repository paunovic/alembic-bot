# alembic-bot

Two small bots that keep alembic migration histories single-headed.

## alembic_bot_pr.py

Runs against github. Lists open PRs, finds migrations whose
down_revision no longer points at the master head, and fixes
them: merges master into the PR, repoints the revision, renumbers
sequential ids, pushes.

## alembic_bot_gocd.py

Runs on master (gocd or any ci). Walks every alembic tree, and
when a merge leaves two heads behind, writes a merge-heads
migration, commits, posts a slack note. A dynamodb lock keeps two
instances from racing.
