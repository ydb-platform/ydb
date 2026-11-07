# Remote CI: platform dispatcher

`dispatch_remote.yml` is a reusable `workflow_call` for Run-and-debug, PR-check,
or any other caller.

## Hard rule

**Remote host never reads platform S3.** Run context (`code_ref`, `job_inputs`,
`runs_on`, `platform_run_id`, …) comes only from the GitHub Actions artifact
`remote-ticket` on the platform run (via webhook → host VM →
`workflow_dispatch`). Host writes claims and build outputs to **host** public
S3 only; platform waiter/ingest are the ones that read that host bucket.

## Platform flow

1. **enqueue** — upload Actions artifact `remote-ticket` (full resolve config;
   webhooks omit `workflow_dispatch` inputs).
2. **wait** (runner-waiter) — poll `remote-claims/{platform_run_id}.json` on
   **host** public S3, then poll the host Actions run to completion.
3. **ingest** — copy host artifacts into platform S3 / analytics.

Platform never starts the host workflow. After the GitHub webhook, the host VM
downloads `remote-ticket` (Actions API) and dispatches `ci_remote_executor`
with **`ticket.config.inputs` only**. Enqueue stamps
`platform_run_id` / `platform_run_attempt` / `platform_repository` into
`config.inputs` from live `github.run_*`. Top-level ticket fields are for
humans/logs.

The remote executor does **not** post GitHub commit statuses (`build_*` /
`test_*`): when `source_run_id` is set, `build_and_test_ya` skips those curls.
The caller run's waiter/ingest outcome is the signal.

Host durable publish (`job_summary.md` + `remote_artifacts.json` at run root)
lives in `ci_remote_executor.yml`, not in the shared `build_and_test_ya` action.

Host S3: `host_s3_endpoint` / `host_s3_bucket`.

Layout under `.github/scripts/ci/`:

- `dispatcher/` — platform only (resolve, enqueue/wait/cancel, ingest)
- `remote_executor/` — host/executor build helpers (e.g. artifacts manifest)

Build config: `dispatcher/resolve_remote_execution.py` → opaque JSON into
`dispatch_remote.yml`.
