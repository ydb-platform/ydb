# Remote CI: platform dispatcher

`dispatch_remote.yml` is a reusable `workflow_call` for Run-and-debug, PR-check,
or any other caller.

## Hard rule

**Remote host never reads platform S3.** Run context comes only from the GitHub
Actions artifact `remote-ticket` on the platform run (via webhook → host VM →
`workflow_dispatch`). Host writes claims and build outputs to **host** public
S3 (`host_s3_endpoint` / `host_s3_bucket`); platform waiter/ingest read that
host bucket.

## Platform flow

1. **enqueue** — upload Actions artifact `remote-ticket` (full resolve config;
   webhooks omit `workflow_dispatch` inputs). Enqueue stamps
   `platform_run_id` / `platform_run_attempt` / `platform_repository` into
   `config.inputs` from live `github.run_*`.
2. **wait** (runner-waiter) — poll `remote-claims/{platform_run_id}.json` on
   host public S3, then poll the host Actions run to completion.
3. **ingest** — copy host artifacts into platform S3 / analytics.

Platform never starts the host workflow. The host VM downloads `remote-ticket`
and dispatches `ci_remote_executor` with **`ticket.config.inputs` only**
(`code_ref`, `job_inputs`, `runs_on`, plus stamped `platform_run_*`). Top-level
`repository` / `workflow_file` / `executor_ref` / `workflow_name` are for the
VM + waiter (GH poll) and logs, not for `workflow_dispatch` inputs.

Host runs as `workflow_dispatch`, so PR-only `build_*` / pending statuses never
fire; `test_*` is skipped when the action gets `source_run_id`
(`= platform_run_id`). The caller run's waiter/ingest outcome is the signal.

Host durable publish (`job_summary.md` + `remote_artifacts.json` at run root)
lives in `ci_remote_executor.yml`, not in the shared `build_and_test_ya` action.

Layout under `.github/scripts/ci/`:

- `dispatcher/` — platform only (resolve, enqueue/wait/cancel, ingest)
- `remote_executor/` — host/executor build helpers (e.g. artifacts manifest)

Build config: `dispatcher/resolve_remote_execution.py` → opaque JSON into
`dispatch_remote.yml`.

## Reruns

- **Re-run failed jobs** — if `enqueue` already succeeded, it is not rerun. The
  waiter pins to `needs.enqueue.outputs.ticket_attempt` (the attempt stamped into
  the existing `remote-ticket`) and re-attaches to that host executor run, then
  re-ingests. It does **not** start a new remote execution.
- **Re-run all jobs** — reruns `enqueue`, writes a new ticket with a new
  `platform_run_attempt`, and the host claims that attempt. Use this when you
  need a fresh remote build/test.

## Limits (operator notes)

### Single dispatch per platform run

Artifact name, claim key and host concurrency are keyed only by
`platform_run_id`. Do not call `dispatch_remote.yml` more than once per run
(e.g. from a matrix) until a `dispatch_key` exists on both platform and host.

### VM contract (required, outside these PRs)

- Verify webhook signature, source repo and workflow allowlist.
- Take `platform_run_id` / attempt from the webhook payload, not from ticket
  fields alone.
- Pick the ticket matching the webhook attempt (highest / newest).
- Serialize `job_inputs` object → JSON string for `workflow_dispatch`.
- Enforce a `runs_on` allowlist.
- Reject fork `head_repository`.

### Cancel

Platform cancel calls the host Actions API: `GH_TOKEN` needs `actions: write`
on the host repo.

### Claim timeout

The waiter fails if no matching claim appears within ~60 min
(`CLAIM_WAIT_ATTEMPTS`, default 120 × 30s; repo var
`REMOTE_CLAIM_WAIT_ATTEMPTS`). Executor wait stays 2400 × 30s
(`EXECUTOR_WAIT_ATTEMPTS`).
