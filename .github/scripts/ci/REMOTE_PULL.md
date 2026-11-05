# Remote CI handoff: pull (no cross-repo launch PAT)

Alternative to push `workflow_dispatch` with a PAT on the remote host repo.

The remote host merges a poller once. It reads public tickets, starts the
executor with its own `github.token`, and writes claims to **its** public S3.
Platform never dispatches Actions on the remote repo; the remote host never
writes to platform/YC S3.

## Objects

| Object | Where | Who writes |
|--------|--------|------------|
| `remote-tickets/{platform_run_id}.json` | platform public S3 (YC) | platform enqueue |
| `remote-claims/{platform_run_id}.json` | remote host public S3 | remote poller |
| artifacts / `remote_artifacts.json` | remote host public S3 | executor |

`platform_run_id` = caller `github.run_id`.

## Flow

1. Platform puts ticket (full resolve `config` inside).
2. Remote poller: new ticket → local `workflow_dispatch` executor → claim on remote S3.
3. Platform waiter: poll claim → poll executor conclusion → ingest.

Config: `handoff: "pull"` (default) or `"push"` (legacy PAT dispatch).
