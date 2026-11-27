# Meta

Contains Meta service code.

Meta service provides HTTP endpoints with cluster and database metadata.
It aggregates information about clusters, balancers, and related database state,
and returns it in a format suitable for UI and service-to-service usage.

## Service endpoint

`meta.service_endpoint` is the stable HTTP(S) base URL of a regional Meta service,
shared by all its replicas, for example `https://meta.eu-north1.example.net`.
It is required when `meta.healthcheck.enabled` is true, including on replicas
with `collect: false`. Otherwise it is optional and defaults to an empty string.
It is not inferred from the host name.
It is distinct from `meta.meta_api_endpoint` (the YDB endpoint of the Meta database)
and the automatically derived per-instance `LocalEndpoint` used for cache forwarding.
The value is available as `TMetaSettings::ServiceEndpoint`.

The per-pod `LocalEndpoint` must resolve and be reachable from peer pods for
owner forwarding to work. Verify pod DNS and network reachability before enabling
healthchecks; keep this address distinct from the shared regional service URL.

The `meta_endpoint` Utf8 column in `ydb/MasterClusterExt.db` assigns each cluster
to its serving regional Meta service. Collection requires an exact string match
between that column and `meta.service_endpoint`; use the same URL spelling,
including any trailing slash. Clusters without an assignment, or assigned to a
different service, are skipped by the local collector.

## Meta DB Tables

- `ydb/MasterClusterExt.db` - primary source of cluster and database metadata used by Meta handlers.
- `ydb/Forwards.db` - used for short-lived forwarding/cache ownership coordination between Meta instances.
- `ydb/MasterClusterVersions.db` - stores version-to-color mapping (`version_str` -> `color_class`) used for cluster version visualization.
- `ydb/DatabaseHealth.db` - latest successfully persisted database status and timestamp per `(Cluster, Database)`.
- `ydb/ClusterHealth.db` - cluster healthcheck status, timestamps, and collector lease/forwarding information. Its `Status` column holds the cluster's `self_check_result`, not a root-database status row.

`schema/database_health.sql` creates both health tables. The separate migration
`schema/cluster_meta_endpoint.sql` adds `meta_endpoint` to the cluster registry.
Apply the required schema changes manually before enabling healthchecks; Meta
does not run these migrations automatically.

When upgrading from the status-only prototype, `DatabaseHealth.db` already
exists. Execute only the new `ClusterHealth.db` CREATE portion of
`schema/database_health.sql`; do not drop or recreate `DatabaseHealth.db`.
Apply the separate `schema/cluster_meta_endpoint.sql` registry migration as well.

## Background cluster healthchecks

Healthcheck collection and endpoints are disabled by default (`enabled: false`).
Before enabling them, prepare the intended Meta database as described above,
assign clusters through `MasterClusterExt.db.meta_endpoint`, and grant collectors
read access to the registry and read/write access to both health tables.
The Meta database token must be a configured service token, not a caller token.

The following configuration lists the defaults with an example regional endpoint;
set `enabled: true` only after completing that deployment preparation:

```yaml
meta:
  # ...existing connection and token settings...
  service_endpoint: https://meta.eu-north1.example.net
  meta_database_token_name: "meta-service-token"
  healthcheck:
    enabled: false
    collect: true
    check_period_seconds: 15
    discovery_period_seconds: 60
    status_ttl_seconds: 120
    request_timeout_seconds: 30
```

`collect` defaults to true and can remain enabled on all replicas sharing the
regional Meta database and `service_endpoint`. Use `enabled: true, collect: false`
for replicas that serve reads and forward requests but do not collect.

### Assignment, ownership, and scheduling

The collector refreshes the cluster registry every `discovery_period_seconds`
(60 seconds by default). It only attempts to acquire clusters assigned to its
regional service. There is no separate `/tenants` discovery or automatic
per-database healthcheck polling.

Replicas coordinate through leases in `ClusterHealth.db`, following the ydb-em
ownership approach:

- `Owner` is a UUID unique to a monitor incarnation, not the shared service URL.
- `ForwardEndpoint` is the owner's per-pod `LocalEndpoint`.
- `ServiceEndpoint` is the shared regional `meta.service_endpoint`.
- `LastCheck` is a database-generated lease timestamp, renewed every 30 seconds.
  The lease lasts 120 seconds; it is not the timestamp of the last successful HC.

This prevents routine duplicate collection but does not guarantee even load
balancing: one replica can own all available clusters. After a failed owner,
takeover takes roughly 120 seconds plus the polling delay. A monitor stops
scheduling checks and publishing results when its lease expires or is lost.
Status writes are fenced in SQL by the current owner, an unexpired lease, and
the current registry assignment/cluster configuration, so obsolete monitors
cannot publish late results.

The owner aims to start one check per cluster every `check_period_seconds`
(15 seconds by default), measured start-to-start. There is at most one active
check/status-write cycle per cluster; a slow cycle delays the next check instead
of accumulating overlapping work. There is no global concurrency cap.
The former `max_requests_per_cluster` option is no longer supported; its protobuf
field 8 is reserved. The configured period is a target, not a freshness guarantee.

### Collection and persistence

For each owned cluster, Meta makes one cluster-wide request using the existing
viewer URL/authentication conventions and the cluster's configured `api_user_token`:

```http
GET /healthcheck?verbose=true&return_hints=true&timeout=30000
```

There is no `database`, `max_level`, or `merge_records` parameter. This keeps the
proxy's default non-merged issue format and retains the full issue details;
`verbose=true` additionally provides `database_status`, including healthy databases.
`request_timeout_seconds` sets the HC operation timeout (30 seconds by default,
sent as 30000 milliseconds). The outer HTTP timeout includes a 20% plus one-second
grace period: 37 seconds with the defaults.

The transport timeout measures inactivity, not total request duration. The monitor
also enforces an absolute deadline (37 seconds by default): it marks the request
expired and rejects late data, but retains its slot until the transport replies.
This does not guarantee cancellation at the deadline. Slow, trickling responses
can delay that cluster further; no new same-cluster request starts while the old
one is draining. Other clusters continue independently.

Meta persists compact statuses from `database_status[].overall` and their
observation timestamps in `DatabaseHealth.db`. The cluster-wide `self_check_result`
is persisted separately in `ClusterHealth.db.Status`; it is not copied
into a synthetic root-database row. Failed collection or persistence does not
overwrite the previous successful snapshot or advance its freshness timestamp.

The full original JSON response is kept only in the owning monitor's RAM, never
in the Meta database. A new snapshot becomes available only after the status
transaction succeeds. On check failure, the previous snapshot remains available
until `status_ttl_seconds` expires (120 seconds by default); afterwards the cached
HC endpoint returns 503. A restart or takeover starts with a cold RAM cache and
returns 503 until the next successful check and status transaction. Database rows
can still serve status reads while they remain fresh.

Upstream cluster-wide HC excludes shared-serverless databases. Meta does not
fabricate GOOD/Green for missing entries or automatically fall back to per-database
checks. Missing or stale database statuses use the existing viewer fallback.

### Viewer response enrichment

`/meta/cp_databases` (including `database_name` filtering) overrides the viewer's
`Overall` only with a known, fresh status. `/meta/cp_databases_verbose` inherits
that override. New rows use the HC color flags directly: GREEN → Green,
BLUE → Blue, YELLOW → Yellow, ORANGE → Orange, RED → Red, GREY → Grey.
Legacy rows remain readable: GOOD → Green, DEGRADED → Yellow,
MAINTENANCE_REQUIRED/EMERGENCY → Red. Missing, expired, unknown statuses and
status-table read errors (including a two-second timeout) leave the viewer value
unchanged. Freshness is checked again when assembling the response. No other viewer fields are changed;
full healthcheck details use the separate authorized endpoint described below.

Status reads use the existing Meta database authentication policy and require
read permission on the status table. Enrichment does not add databases not already
returned by viewer. Meta's incoming cache for `cp_databases*` is disabled when
this feature is enabled, so it cannot retain an expired override.

### Statuses for a UI that queries viewer directly

When `healthcheck.enabled` is true, Meta advertises capability
`/meta/database_statuses: 1` in `/capabilities` and exposes:

```http
GET /meta/database_statuses?cluster_name=my-cluster
Authorization: Bearer <user-token>
```

For each request Meta resolves the cluster's `balancer` in
`MasterClusterExt.db` and calls its viewer `/whoami` endpoint, without a
`database` parameter. It forwards the caller's `Authorization` and `Cookie`
headers unchanged (also `x-yacloud-subjecttoken` and `x-request-id`).
Configure `balancer` to use the appropriate OIDC/proxy route when required:
OIDC remains responsible for token exchange/substitution or passthrough.
Meta does not substitute the collector's `api_user_token` for this check.

Only an HTTP 200 JSON object with the **boolean** `IsViewerAllowed: true`
permits reading `DatabaseHealth.db`. An OIDC profile alone, a missing flag,
`IsDatabaseAllowed: true`, or a string `"true"` does not grant access. This
uses the cluster's viewer access policy, including its anonymous-access
configuration; it is not a per-database ACL check or authorization for full
healthcheck details. No healthcheck statuses or database names are returned
if this check fails. Meta does not forward the whoami profile to the client.

After a successful `/whoami` check, the endpoint forwards the request to the
assigned regional Meta service if `MasterClusterExt.db.meta_endpoint` identifies
a different service. Otherwise it reads the statuses from the local Meta database.
Regional forwarding does not bypass caller authorization.

Example response:

```json
{
  "cluster_name": "my-cluster",
  "databases": [
    {
      "Name": "/Root/db",
      "Overall": "Yellow",
      "HealthcheckStatus": "YELLOW",
      "CheckedAt": "2026-10-20T12:00:00.000000Z",
      "ExpiresAt": "2026-10-20T12:02:00.000000Z"
    }
  ]
}
```

For new records, `HealthcheckStatus` is the original
GREEN/BLUE/YELLOW/ORANGE/RED/GREY flag from `database_status[].overall`, and
`Overall` is the corresponding title-case viewer flag. Legacy GOOD/DEGRADED/
MAINTENANCE_REQUIRED/EMERGENCY rows are also supported with the mapping above.
These database color flags are not the cluster's `self_check_result`.

Timestamps are UTC ISO 8601. Only known, fresh statuses are included; future
timestamps and rows expired at response assembly are omitted. An empty array
is a successful authorized read with no fresh statuses, not a healthy-cluster
signal. The UI can merge by `Name` into its existing viewer database list,
use `Overall` until `ExpiresAt`, and otherwise retain the viewer status.
The endpoint does not trigger healthchecks or return their full responses.

The handler bypasses Meta's incoming caches and returns `Cache-Control:
no-store` on both success and error, so authorization runs for every caller
and request. Reads follow the existing Meta database credential policy and
need read access to `MasterClusterExt.db` and `DatabaseHealth.db`.
A missing cluster name returns 400, an unknown cluster 404,
viewer authentication/authorization denial 401/403,
an invalid/unavailable whoami response 502, a metadata read failure 503, and
the overall request deadline 504. Whoami has a five-second timeout; each
metadata query has a two-second timeout; the overall deadline is ten seconds.
Login redirects are not followed. Only GET and CORS OPTIONS are supported.

### Cached full healthcheck response

```http
GET /meta/healthcheck?name=my-cluster
Authorization: Bearer <user-token>
```

The `cluster_name` query parameter is also accepted as an alias for `name`.

This endpoint returns the original cached cluster HC JSON, without transforming
or wrapping it. It never triggers an on-demand healthcheck. Responses use
`Cache-Control: no-store`; successful responses include freshness headers
`X-Healthcheck-Checked-At` and `X-Healthcheck-Expires-At`. A missing, expired, or
cold snapshot returns 503 rather than an apparently healthy or stale response.

Full-response access checks the cluster viewer's `/whoami` with the caller's
authentication headers/cookies through the configured OIDC route. OIDC handles
token exchange/substitution or passthrough; Meta does not replace caller
credentials with the collector token. Access requires an HTTP 200 JSON response
with the boolean `IsMonitoringAllowed: true`. `IsViewerAllowed` alone is not
sufficient for full HC details, and a string `"true"` is not accepted.
The separate database-status endpoint continues to require `IsViewerAllowed`.

Requests are routed to the assigned regional Meta service when necessary, then
to the owner pod using the live lease's `ForwardEndpoint`. Forwarding is bounded
to prevent loops; the regional endpoint and the per-pod endpoint must both be
resolvable and reachable from forwarding replicas. The shared `service_endpoint`
must not replace the owner's per-pod `LocalEndpoint`. Regional routing and lease
lookup do not bypass caller authorization or extend a snapshot's expiry.
The handler rechecks freshness on forwarded replies before returning them to the
caller; a snapshot that expires during forwarding is rejected with 503.

### Future healthcheck-proxy integration

Integrating healthcheck-proxy requires separate proxy configuration/code and
deployment changes. Pointing it at the cached endpoint allows monitoring and UI
to reuse one cluster collection cycle. That migration must preserve the original
cluster `BALANCER` label: the proxy currently derives it from the source URL, which would
otherwise identify Meta. It must also handle 503 for stale/cold caches and the
freshness headers, rather than treating unavailable or old data as healthy.

## Config Examples

Generic auth/access_service_type examples are shared in:

- `../core/examples/simple_config.yaml`
- `../core/examples/static_config.yaml`
- `../core/examples/federated_config.yaml`
- `../core/examples/token_file.pb.txt`

Meta-specific examples from `examples/`:

- `examples/config.yaml` - `generic` + `meta` configuration using `meta_database_token_name: federated-token`, an example regional service endpoint, and disabled-by-default healthcheck settings.
- `examples/support_links_config.yaml` - support links example with `grafana/dashboard` and `grafana/dashboard/search` sources. Requires `meta_database_token_name` and `grafana.endpoint`.
