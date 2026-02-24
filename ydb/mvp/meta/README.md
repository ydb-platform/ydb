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

Use config examples from `examples/`:

- `examples/simple_config.yaml` - minimal config with direct token usage from token file.
- `examples/static_config.yaml` - config with OAuth exchange settings in YAML and private key loaded from token file by token name.
- `examples/federated_config.yaml` - config with federated-style subject/actor credentials in YAML.
- `examples/token_file.pb.txt` - protobuf text example for `generic.auth.token_file`.
