# Remote CI: platform dispatcher

Run-and-debug with `executor=remote` → `resolve_remote_execution.py` →
`dispatch_remote.yml` (enqueue → wait → ingest).

**Hard rule:** the remote host never reads platform S3. Its launch context comes
only from the `remote-ticket` Actions artifact; claims and build outputs go to
**host** public S3 (`host_s3_endpoint` / `host_s3_bucket`).

## Pinned remote JSON

`repository`, `workflow_file`, `host_s3_endpoint`, and `host_s3_bucket` always
come from `DEFAULT_REMOTE` in `resolve_remote_execution.py` (`nebius/ydb`,
`ci_remote_executor.yml`, and the known host bucket). A caller value that
differs is rejected. Repeating the default is allowed. Other keys
(`workflow_name`, `executor_ref`, `runs_on`, `code_ref`, `analytics_branch`,
`job`) may still be overridden.

Remote accepts **one** `build_preset`. A comma-separated list in
`remote.job.build_preset`, or in the run-and-debug `build_preset` input when
that job key is omitted, is an error. The ydb_yc default
`relwithdebinfo, release-asan` is therefore rejected for `executor=remote`
unless `remote.job.build_preset` is a single preset.

`job.collect_coredumps` defaults to `true`, matching `run_tests`. Coredumps
stay on a local loop volume (`/coredumps`) for ya recovery. They are not part
of the host S3 contract.

## Host launch

The host calls `build_and_test_ya` with that action's existing inputs only.
It does not pass `enable_ydb_qa`, `source_run_id`, or `analytics_job_name`, and
it does not read `action.yml` to discover inputs. Host job `env` (set on the
host workflow, not composite inputs):

| env | effect |
| --- | --- |
| `CI_REMOTE_EXECUTOR=1` | action skips the test commit-status post |
| `CI_DISABLE_YDB_QA=1` | action and scripts skip YDB QA (exit 0, no DB open) |

The host publishes `remote_artifacts.json` after `build_and_test_ya` finishes
(including composite cleanup). It lists objects under `S3_BUCKET_PATH` with
`s3cmd ls -r`. An object line's URI is the substring from `s3://` through the
end of the line — not the last whitespace token. The workflow name
`CI remote executor` contains spaces and is part of the object key.
`parse_s3cmd_ls_line` in `s3cmd_ls.py` is the platform copy of that rule; the
host workflow inlines the same function, because the checked-out platform SHA
may predate this helper. Ingest copies listed objects. `report.json` and
`index.html` are used when the host listed them.

`ci_metrics.jsonl`, `started_at`, and test-results upload are **optional
best-effort**. The host workflow does not promise those files. This path does
not add `started_at` or metrics writes to `build_and_test_ya`. If a listed tree
happens to contain them, ingest flushes metrics and uploads test results. If
they are absent, ingest skips those steps and still succeeds.

## Flow

1. **enqueue** — upload `remote-ticket` (resolved config; `platform_run_id` /
   `platform_run_attempt` / `platform_repository` stamped into `config.inputs`).
2. Host VM (webhook) dispatches `ci_remote_executor` with `ticket.config.inputs`
   only. Host runs as `workflow_dispatch` (no PR comments). `CI_REMOTE_EXECUTOR=1`
   skips test commit statuses; `CI_DISABLE_YDB_QA=1` skips the QA database.
   The caller run's waiter/ingest is the signal.
3. **wait** (`runner-waiter`) — poll `remote-claims/{platform_run_id}.json` on
   host S3 (up to 120×30s), then the host Actions run. The executor poll is
   `EXECUTOR_WAIT_ATTEMPTS` = 2880×30s (24h), matching the host execute job
   `timeout-minutes: 1440`. The wait job `timeout-minutes` is 1560 so the claim
   window plus a full executor day fit with slack for HTTP overhead (avoids
   cancelling the host when the job hits the GHA timeout ceiling).
4. **ingest** — read host `remote_artifacts.json` (`files` list + metadata),
   anonymous GetObject for each key, copy to platform S3, write a summary.
   Test-results upload and `ci_metrics` flush run only when `started_at` /
   `ci_metrics.jsonl` are actually present (best-effort; missing is normal).
   When a flush does happen, the analytics job name is the caller workflow.

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

## Residual risk (claim vs ticket)

The host claim job checks syntax (`platform_repository` is `ydb-platform/ydb`,
`runs_on` allowlist, digit ids) and then publishes `remote-claims/{id}.json`
from the `workflow_dispatch` inputs. It does not download the platform
`remote-ticket` artifact or verify a signature. That check needs a token that
can read `ydb-platform/ydb` artifacts, which this workflow does not have, and
the ticket has no signed fields. A principal who can dispatch
`ci_remote_executor` can publish a claim for a `platform_run_id`, and the
platform waiter will follow that executor run. The VM webhook checks
(signature, source repo, workflow allowlist) remain the gate. This repo does
not add crypto.
