# Remote CI handoff: pull (no cross-repo launch PAT)

Alternative to push `workflow_dispatch` with a PAT on the remote host repo.

The remote host merges a poller once. It learns about a platform run from a
GitHub webhook (or by listing open runs), downloads the **`remote-ticket`**
Actions artifact from that run (full resolve `config` / executor inputs),
starts the executor with its own `github.token`, and writes claims to **its**
public S3. Platform never dispatches Actions on the remote repo.

Webhook payloads do not include `workflow_dispatch` inputs; the artifact
carries everything the executor needs (`code_ref`, `job_inputs`, `runs_on`, …).

## Objects

| Object | Where | Who writes |
|--------|--------|------------|
| artifact `remote-ticket` (`remote-ticket.json`) | platform Actions run | platform enqueue |
| `remote-claims/{platform_run_id}.json` | remote host public S3 | remote poller |
| artifacts / `remote_artifacts.json` | remote host public S3 | executor |

`platform_run_id` = caller `github.run_id`.

Optional legacy: if `ticket_s3_*` is set, enqueue also mirrors the same JSON to
`remote-tickets/{platform_run_id}.json` on platform public S3.

## Flow

1. Platform enqueue writes ticket JSON and uploads artifact `remote-ticket`
   (available as soon as upload finishes, while the waiter is still running).
2. Remote poller: webhook/`PLATFORM_RUN_ID` or open platform runs → download
   artifact (needs a token with `actions:read` on the platform repo) → local
   `workflow_dispatch` executor → claim on remote S3.
3. Platform waiter: poll claim → poll executor conclusion → ingest.

Config: `handoff: "pull"` (default) or `"push"` (legacy PAT dispatch).

## Poller secrets / env

| Name | Role |
|------|------|
| `GH_TOKEN` | this (executor) repo — local dispatch + claims tooling |
| `PLATFORM_GH_TOKEN` | read artifacts/runs on platform repo (fine-grained PAT or app) |
| `PLATFORM_REPOSITORY` | e.g. `ydb-platform/ydb` |
| `PLATFORM_RUN_ID` | optional; single run from webhook (else list open runs) |
| `CLAIM_S3_BUCKET` / `CLAIM_PUBLIC_BASE` | remote public S3 for claims |
| `TICKET_BASE_URL` | optional S3 ticket fallback |
| `WORKFLOW_FILE` / `EXECUTOR_REF` | executor workflow defaults |
