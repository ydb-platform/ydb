# OIDC Proxy

Contains OIDC proxy service code.

## Config Examples

Generic auth/access_service_type examples are shared in:

- `../core/examples/simple_config.yaml`
- `../core/examples/static_config.yaml`
- `../core/examples/federated_config.yaml`
- `../core/examples/token_file.pb.txt`

OIDC proxy-specific examples from `examples/`:

- `examples/y_config.yaml` - `yandex_v2` minimal `generic` + `oidc` block.
- `examples/n_config.yaml` - `nebius_v1` minimal `generic` + `oidc` block using `session_service_token_name: federated-token`.
