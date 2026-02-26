# OIDC Proxy

Contains OIDC proxy service code.

## Config Examples

Use `examples/` for ready-to-run configs:

- `examples/simple_config.yaml` - minimal config with direct token usage from token file.
- `examples/static_config.yaml` - config with static OAuth2 token exchange credentials in YAML.
- `examples/federated_config.yaml` - config with federated-style subject/actor credentials in YAML.
- `examples/token_file.pb.txt` - protobuf text example for `generic.auth.token_file`.
