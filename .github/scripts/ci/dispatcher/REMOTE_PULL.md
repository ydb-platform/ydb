# Remote CI: platform dispatcher

Run-and-debug with `executor=remote` → `resolve_remote_execution.py` →
`dispatch_remote.yml` (enqueue → wait → ingest).

**Hard rule:** the remote host never reads platform S3. Its launch context comes
only from the `remote-ticket` Actions artifact; claims and build outputs go to
**host** public S3 (`host_s3_endpoint` / `host_s3_bucket`).

The host calls `build_and_test_ya` with that action's existing inputs only.
It does not pass `enable_ydb_qa`, `source_run_id`, or `analytics_job_name`, and
it does not read `action.yml` to discover inputs. Host job `env` (set on the
host workflow, not composite inputs):

| env | effect |
| --- | --- |
| `CI_REMOTE_EXECUTOR=1` | action skips the test commit-status post |
| `CI_DISABLE_YDB_QA=1` | action and scripts skip YDB QA (exit 0, no DB open) |

The host publishes `remote_artifacts.json` itself after `build_and_test_ya`
finishes (including composite cleanup): it lists objects already synced under
`S3_BUCKET_PATH` via `s3cmd ls`, not a local `PUBLIC_DIR` tree. Ingest copies
listed objects. `report.json` and `index.html`
are used when the host listed them. `ci_metrics.jsonl` and `started_at` are
optional: metrics flush and test-results upload run only when those files exist.

## Flow

1. **enqueue** — upload `remote-ticket` (resolved config; `platform_run_id` /
   `platform_run_attempt` / `platform_repository` stamped into `config.inputs`).
2. Host VM (webhook) dispatches `ci_remote_executor` with `ticket.config.inputs`
   only. Host runs as `workflow_dispatch` (no PR comments). `CI_REMOTE_EXECUTOR=1`
   skips test commit statuses; `CI_DISABLE_YDB_QA=1` skips the QA database.
   The caller run's waiter/ingest is the signal.
3. **wait** (`runner-waiter`) — poll `remote-claims/{platform_run_id}.json` on
   host S3 (~60 min), then the host Actions run to completion.
4. **ingest** — read host `remote_artifacts.json` (`files` list + metadata),
   anonymous GetObject for each key, copy to platform S3, upload test results
   when `report.json` and `started_at` are present, flush `ci_metrics` when the
   file is present, write summary. Job name for analytics is the caller workflow.

## Reruns

- **Re-run failed jobs** — `enqueue` is not rerun; the waiter pins to
  `needs.enqueue.outputs.ticket_attempt` and re-attaches to that host run.
- **Re-run all jobs** — new ticket attempt → fresh remote build/test.

## Limits

- One `dispatch_remote.yml` call per platform run (artifact, claim key and host
  concurrency are keyed by `platform_run_id`).
- Cancel needs `GH_TOKEN` with `actions: write` on the host repo.
- VM contract (outside this repo):
  - verify webhook signature, source repo and workflow allowlist;
  - take `platform_run_id` / attempt from the webhook payload, not from ticket
    fields alone; pick the ticket matching that attempt (highest / newest);
  - serialize the `job_inputs` object → JSON string for `workflow_dispatch`;
  - enforce a `runs_on` allowlist; reject fork `head_repository`.
