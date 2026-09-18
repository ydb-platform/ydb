# Meta

Contains Meta service code.

Meta service provides HTTP endpoints with cluster and database metadata.
It aggregates information about clusters, balancers, and related database state,
and returns it in a format suitable for UI and service-to-service usage.

## Meta DB Tables

- `ydb/MasterClusterExt.db` - primary source of cluster and database metadata used by Meta handlers.
- `ydb/Forwards.db` - used for short-lived forwarding/cache ownership coordination between Meta instances.
- `ydb/MasterClusterVersions.db` - stores version-to-color mapping (`version_str` -> `color_class`) used for cluster version visualization.

## Cluster redirects

`/cluster/<cluster_name>/<path>` returns a `307 Temporary Redirect` to the
cluster's `balancer` URL from `ydb/MasterClusterExt.db`. This field must contain
an absolute HTTP(S) URL, including the OIDC proxy prefix when one is used.
The handler removes the trailing `/viewer/json` or `/viewer` from the stored
URL, then appends the requested path and query without changing their encoding.

For example, with `balancer` set to
`https://oidc.example.net/storage.example.net:8765/viewer/json`:

```text
/cluster/testing-global/viewer/json/nodes?limit=80
  -> https://oidc.example.net/storage.example.net:8765/viewer/json/nodes?limit=80
```

The redirect preserves the HTTP method and body. The browser sends the next
request to the destination, where the existing authentication and CORS rules
apply. Meta does not proxy the cluster response or add a service token.
Through the website's `/api/meta` route, the example starts with
`/api/meta/cluster/testing-global/viewer/json/nodes?limit=80`.

Each pod caches the cluster name to balancer mapping in memory. Concurrent
requests for a cluster with an empty cache share one database lookup. After
the first successful lookup, the pod refreshes the entry in the background
60 seconds after each completed attempt, without requiring another request.
Requests use the saved address during refreshes and after lookup errors,
including an invalid balancer URL. A successful lookup that no longer finds
the cluster replaces its saved address with a `404` result. Entries are removed
after seven days without requests; restarting the pod clears the cache.

The cache holds only routing information, not request bodies or cluster API
responses. Each redirect uses the current request's path and query. The service
token is obtained again for each lookup; if meta uses caller credentials instead,
cache entries are separated by token. This cache is local regardless of `MetaCache`.

Redirect responses use `Cache-Control: no-store`. Target query parameters such as
`database` do not change which database meta reads. Unknown clusters return
`404`; without a cached address, missing or invalid balancer URLs return `503`
and a meta lookup timeout returns `504`. Invalid paths, including
parent-directory traversal, return `400`.

## Config Examples

Generic auth/access_service_type examples are shared in:

- `../core/examples/simple_config.yaml`
- `../core/examples/static_config.yaml`
- `../core/examples/federated_config.yaml`
- `../core/examples/token_file.pb.txt`

Meta-specific examples from `examples/`:

- `examples/config.yaml` - minimal `generic` + `meta` block using `meta_database_token_name: federated-token`.
- `examples/support_links_config.yaml` - support links example with `grafana/dashboard` and `grafana/dashboard/search` sources. Requires `meta_database_token_name` and `grafana.endpoint`.
