# Meta

Contains Meta service code.

Meta service provides HTTP endpoints with cluster and database metadata.
It aggregates information about clusters, balancers, and related database state,
and returns it in a format suitable for UI and service-to-service usage.

## Meta DB Tables

- `ydb/MasterClusterExt.db` - primary source of cluster and database metadata used by Meta handlers.
- `ydb/Forwards.db` - used for short-lived forwarding/cache ownership coordination between Meta instances.
- `ydb/MasterClusterVersions.db` - stores version-to-color mapping (`version_str` -> `color_class`) used for cluster version visualization.

## Config Examples

Generic auth/access_service_type examples are shared in:

- `../core/examples/simple_config.yaml`
- `../core/examples/static_config.yaml`
- `../core/examples/federated_config.yaml`
- `../core/examples/token_file.pb.txt`

Meta-specific examples from `examples/`:

- `examples/config.yaml` - minimal `generic` + `meta` block using `meta_database_token_name: federated-token`.
