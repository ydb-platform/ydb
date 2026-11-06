# Remote CI handoff: pull (no cross-repo launch PAT)

Default handoff: platform uploads Actions artifact `remote-ticket` (full resolve
config). Remote poller reads it by `platform_run_id` (webhook or open-run scan),
starts the local executor, writes a claim on **its** public S3. Platform waiter
polls the claim, then ingest.

GitHub webhooks omit `workflow_dispatch` inputs — the artifact carries them.

## Objects

| Object | Where | Who writes |
|--------|--------|------------|
| artifact `remote-ticket` | platform Actions run | platform enqueue |
| `remote-claims/{platform_run_id}.json` | remote public S3 | remote poller |
| artifacts / `remote_artifacts.json` | remote public S3 | executor |

`platform_run_id` = caller `github.run_id`.

Optional: set `ticket_s3_*` to also mirror the JSON to platform S3.

## Poller secrets / env

| Name | Role |
|------|------|
| `GH_DISPATCH_TOKEN` → `GH_TOKEN` | PAT/app on **this** repo for `workflow_dispatch` (`github.token` will not start workflows) |
| `PLATFORM_GH_TOKEN` | `actions:read` on platform repo (artifacts / runs) |
| `PLATFORM_REPOSITORY` | e.g. `ydb-platform/ydb` |
| `PLATFORM_RUN_ID` | optional single run; if set, poller waits for the artifact |
| `CLAIM_S3_BUCKET` / `CLAIM_PUBLIC_BASE` | remote public S3 for claims |
| `TICKET_BASE_URL` | optional S3 ticket fallback |
| `EXECUTOR_REF` | default branch if ticket omits `executor_ref` |

Config: `handoff: "pull"` (default) or `"push"` (legacy PAT dispatch).
