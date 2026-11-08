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
VM + waiter (GH poll), not for `workflow_dispatch` inputs. Top-level ticket
fields are also for humans/logs.

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
