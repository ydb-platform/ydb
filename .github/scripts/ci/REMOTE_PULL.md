# Remote CI handoff: pull (platform)

Default `handoff: pull`: platform enqueue uploads Actions artifact `remote-ticket`
(full resolve config). A remote-host poller (not in this repo) downloads it by
`platform_run_id`, starts the executor, and writes `remote-claims/{id}.json` on
**remote** public S3. Platform waiter follows the claim, then ingest.

Webhooks omit `workflow_dispatch` inputs — the artifact carries them.

## Platform objects

| Object | Where |
|--------|--------|
| artifact `remote-ticket` | this Actions run (enqueue job) |
| claim (read) | remote public S3 (`claim_s3_*` / `summary_s3_*`) |
| ingest | reupload + analytics from remote `remote_artifacts.json` |

`platform_run_id` = caller `github.run_id`.

Optional: `ticket_s3_*` mirrors the same JSON to platform S3.  
`handoff: push` = legacy PAT `workflow_dispatch` to the remote repo.

Host poller / executor live in the Nebius repo (see nebius/ydb). Config builder:
`resolve_remote_execution.py` → `dispatch_remote.yml`.
