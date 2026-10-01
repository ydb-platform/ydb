# Validation and operation

## Focused checks

- `ydb/tools/ydb_bench/tests/test_cluster_templates.py`: schema, migration, revisions, placement,
  CRUD and JavaScript helpers.
- `ydb/tools/ydb_bench/tests/test_ydb_bench.py`: configuration, search, execution and web behavior.
- `ydb/tools/ydb_bench/tests/test_hosts.py`: membership, proxies and federation.
- `ydb/tools/ydb_bench/tests/test_ydb_telemetry.py`: telemetry contracts.
- Native targets such as `ydb/tools/ydb_bench/memory/ut`: only when those components change.

Select affected cases. Run builds/tests remotely through `ssh_ya` under root
build instructions; do not edit sources during compilation. JS tests require
Node.js and may skip without it. Report GOOD and SKIPPED separately; skipped
JS checks are not frontend validation.

For UI acceptance use the real browser: add/edit/cancel, cross-view moves,
save/reopen, copy isolation, validation errors and affected empty states. Use
own QA drafts, not user templates. Do not start benchmark workload merely for
layout testing. Record coverage and gaps; API checks only supplement UI tests.

## Build and deploy

The executable embeds Python assets and native binaries. Check `ydb/tools/ydb_bench/ya.make`:
`profile` retains embedded YDB/YDB CLI symbols; other build types strip them.
An external `ydbd-binary` path is on the selected benchmark host, not the browser
machine, and differs from replacing the benchmark service executable.

When deployment is requested, discover the actual service, output directory,
listener, release path and existing deployment automation. Prefer the existing
workflow; do not invent script locations or hard-code operator host aliases.

Before replacement check `/api/activity-status`, queued/running work and managed
processes. Recheck immediately before stopping the service: idle state can
change during a build. Do not cancel workload to permit deployment without
authorization. Unknown activity state is not permission to restart.

Keep a rollback binary and preserve results, identities, membership, peer
credentials, templates and comparisons. Switch through the existing service
mechanism. Verify the running executable/revision, health, listener and affected
browser behavior. Report rollout status separately for each host.

SSH tunnels only provide browser access; they are distinct from peer links.
Do not broaden listeners, change ports/tokens or remove tunnels as incidental
build steps. Never print peer tokens in reports or logs.
