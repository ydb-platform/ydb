# Distributed Nemesis operations

Paths are relative to the repository root. The CLI and HTTP details below come from ydb/tests/stability/nemesis/__main__.py, ydb/tests/stability/nemesis/internal/orchestrator/install.py, and ydb/tests/stability/nemesis/routers/orchestrator_router.py. Check the deployed revision when its responses differ.

## Build and deploy

1. Build on the selected Linux environment using the active build instructions:

   ```bash
   ./ya make --build relwithdebinfo ydb/tests/stability/nemesis
   ```

2. The resulting executable is ydb/tests/stability/nemesis/nemesis. Keep static/ beside it, or supply an absolute --static-location. The installer resolves the binary and relative assets beside the invoked executable, not the working directory.
3. Use the actual cluster YAML and, for a split configuration, the database template. The topology requires a recognized static_erasure or erasure and host location.rack; mirror-3-dc also requires location.data_center. Hosts/config may be top-level or under config. The first **sorted hostname** is the orchestrator; YAML order does not select it.
4. Inspect the destination before installation: install deploys and starts nemesis-agent.service on every configured host, uses sudo, and defaults to /Berkanavt/nemesis. --install-root changes files, but does not give the service a distinct name. It cannot isolate two installations on the same host by itself.
5. Once this deployment is within the task's scope, use the relevant form (replace example paths):

   ```bash
   ./ydb/tests/stability/nemesis/nemesis --help
   ./ydb/tests/stability/nemesis/nemesis install --yaml-config-location /absolute/cluster.yaml
   ```

   For a split config, append --database-config-location /absolute/databases.yaml. Set --kikimr-logs-directory to the actual YDB logs when the default /Berkanavt/kikimr/logs/ does not apply.

The default UI is http://ORCHESTRATOR:31434/static/index.html. Nemesis sensors default to 8666; the warden monitoring port defaults to 8765. The current CLI parses --mon-port but does not propagate it into settings; do not rely on it to change either endpoint. Installation starts services; check scheduler state separately. The test harness in ydb/tests/library/stability/deploy.py can start chaos as a subsequent step.

## Inspect before starting

Set NEMESIS_URL to the verified orchestrator URL, without a trailing slash. These commands use that variable:

```bash
curl --fail-with-body --silent --show-error --max-time 15 "$NEMESIS_URL/api/inventory"
curl --fail-with-body --silent --show-error --max-time 15 "$NEMESIS_URL/api/process_types"
curl --fail-with-body --silent --show-error --max-time 15 "$NEMESIS_URL/api/scheduler"
curl --fail-with-body --silent --show-error --max-time 15 "$NEMESIS_URL/api/schedule"
curl --fail-with-body --silent --show-error --max-time 15 "$NEMESIS_URL/api/problems"
```

1. Compare nodes/slots and their ports with the stand. Inventory is built at initialization; investigate degraded inventory or stale mappings before enabling affected faults.
2. Check /api/hosts/health for agent reachability and /api/healthcheck for YDB health. Resolve a blind recovery probe before guarded faults; an empty problems list alone is insufficient.
3. /api/scheduler is the boundary scheduler. /api/schedule is an independent legacy per-type scheduler. Use one scheduling path for a campaign and inspect both to avoid overlapping injections.
4. Select only catalog types appropriate to the requested failure model. A start request with omitted enabled retains the current profile, which initially includes disk, network, and clock faults. Never use an empty start body as a smoke check.

## Start and observe a bounded campaign

After a healthy workload baseline, this example enables only DataShard tablet restarts. Confirm the type exists and its cluster-wide targeting fits the experiment before using it:

```bash
curl --fail-with-body --silent --show-error --max-time 30 \
  -H 'Content-Type: application/json' \
  --data '{"enabled":["KillDataShardNemesis"],"base_interval":30,"jitter":0.2,"max_per_tick":1,"max_bypass_per_tick":1}' \
  "$NEMESIS_URL/api/scheduler/start"
```

1. The API has no campaign-duration field. The caller must enforce the chosen deadline and execute shutdown in a finally/cleanup path, including when the start response times out.
2. Tablet faults use BYPASS and do not consume the storage failure budget. They are not restricted to the workload's tables by this profile. Use explicit targeting in a reviewed stand adapter when other databases share the cluster.
3. FULL-guarded faults use separate storage-domain and slot budgets. erasure=none allows no storage-domain failures; compute-slot failures use their own budget. Do not answer a guard rejection with force=true automatically.
4. For manual actions, use the orchestrator POST /api/hosts/process with type, action, and target. Check supports_manual first. Set target.kind to the catalog's target_kind and copy host and required IDs/ports from the matching inventory entity. Inventory rows do not include kind. See ydb/tests/stability/nemesis/internal/orchestrator/nemesis/chaos_target.py for the target schema. Direct agent POST /api/processes bypasses orchestrator planning/accounting.
5. Capture /api/hosts/processes before and after each smoke injection. Find new host-local process id values and require status=finished with ret_code=0; inspect type and logs. These process IDs are not the planner execution_id returned by a manual POST. Run smoke injections one at a time; if concurrent identical actions cannot be correlated, record the outcome as unknown. A successful POST means dispatch, not successful execution. On timeout, observe existing processes before retrying.
6. /api/schedule/history returns only the latest 15 entries. Preserve continuous orchestrator logs and fault timestamps for longer runs; nemesis_metric records describe lifecycle and budget transitions. Monitor /api/problems and /api/scheduler for stuck faults and held leases.

## Stop, recover, then stop services

Do both operations while the orchestrator and agents remain alive:

```bash
curl --fail-with-body --silent --show-error --max-time 60 -X POST "$NEMESIS_URL/api/scheduler/stop"
curl --fail-with-body --silent --show-error --max-time 60 \
  -H 'Content-Type: application/json' --data '{"enabled":false}' \
  "$NEMESIS_URL/api/schedule/all"
```

1. Verify the boundary scheduler is stopped and every legacy schedule disabled. Wait for in-flight actions and extracts; the app-owned recovery probe continues after scheduler stop.
2. Inspect scheduler recovery_probe (tracked, confirming, stuck, blind, faults), failure_budget, host processes, healthcheck, and problems until recovery is confirmed or the experiment's recovery deadline expires. Nonzero tracked/confirming means recovery is pending; blind means health evidence is unavailable. Keep the services alive for pending extracts. Do not clear stuck state or restart the orchestrator merely to erase a held budget.
3. Request the experiment's time window: POST /api/hosts/warden/start with start_time/end_time Unix seconds (or since/until ISO timestamps), then poll GET /api/hosts/warden/results. Match started_at/completed_at to the new check run. Inspect every check's status, violations, errors, and missing hosts; top-level completed does not mean all checks passed. The default requested window is the last 24 hours, but some log wardens ignore it or filter only by file modification time. Check violation timestamps before attributing them to this campaign.
4. Run the independent workload/data audit described in experiments.md. Preserve failure evidence and distinguish recovered availability from correct data.
5. If service shutdown is requested and recovery is complete:

   ```bash
   ./ydb/tests/stability/nemesis/nemesis stop --yaml-config-location /absolute/cluster.yaml
   ```

   The current CLI stop disables the legacy scheduler, waits 30 seconds, and stops agents before the orchestrator. It is not a replacement for the explicit two-scheduler drain above. Verify service states on all targeted hosts: the CLI can print a success summary after individual stop warnings. On a custom stand, stop its actual service instead of the installer's fixed service name.
