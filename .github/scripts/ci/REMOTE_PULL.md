# Remote CI: platform dispatcher

`dispatch_remote.yml` is a reusable `workflow_call` for Run-and-debug, PR-check,
or any other caller:

1. **enqueue** (runner-waiter) — write + upload Actions artifact `remote-ticket`
   (full resolve config; webhooks omit `workflow_dispatch` inputs).
2. **wait** (runner-waiter) — poll `remote-claims/{platform_run_id}.json` on the
   **host** public S3, then poll the host Actions run to completion.
3. **ingest** — pull host artifacts into platform S3 / analytics.

Platform never starts the host workflow. After the GitHub webhook, the host VM
downloads `remote-ticket` and `workflow_dispatch`es `ci_remote_executor` with
`config.inputs`. The executor publishes the claim and build outputs on host S3
only.

`platform_run_id` = caller `github.run_id`.

Build config: `resolve_remote_execution.py` → opaque JSON into `dispatch_remote.yml`.
