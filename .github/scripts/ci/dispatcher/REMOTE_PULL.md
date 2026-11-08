# Remote CI: platform dispatcher

Run-and-debug with `executor=remote` → `resolve_remote_execution.py` →
`dispatch_remote.yml` (enqueue → wait → ingest).

**Hard rule:** the remote host never reads platform S3. Its launch context comes
only from the `remote-ticket` Actions artifact; claims and build outputs go to
**host** public S3 (`host_s3_endpoint` / `host_s3_bucket`).

## Flow

1. **enqueue** — upload `remote-ticket` (resolved config; `platform_run_id` /
   `platform_run_attempt` / `platform_repository` stamped into `config.inputs`).
2. Host VM (webhook) dispatches `ci_remote_executor` with `ticket.config.inputs`
   only. Host runs as `workflow_dispatch` with `source_run_id = platform_run_id`
   (no PR comments / commit statuses); the caller run's waiter/ingest is the signal.
3. **wait** (`runner-waiter`) — poll `remote-claims/{platform_run_id}.json` on
   host S3 (~60 min), then the host Actions run to completion.
4. **ingest** — read host `remote_artifacts.json` (`files` list + metadata),
   anonymous GetObject for each key, copy to platform S3, upload test results and
   `ci_metrics` to YDB QA, write summary.

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
