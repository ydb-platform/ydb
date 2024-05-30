# Volatile configurations

Volatile configurations are a special type of configuration that complements dynamic configurations while being non-persistent. These configurations are discarded when the Console [tablet](../../concepts/cluster/common_scheme_ydb.md#tablets) is moved or restarted, as well as when the main configuration is updated.

Primary use cases:

- Temporarily changing configuration for debugging or testing
- Trial activation of potentially dangerous settings. In the event of a cluster crash or restart, these settings will be automatically disabled

These configurations are added at the end of the selectors set, and the syntax for their description is identical to the [selector syntax](./dynamic-config-selectors.md).

```bash
# Retrieve all volatile configurations uploaded to the cluster
{{ ydb-cli }} admin volatile-config fetch --all --output-directory <dir>
# Retrieve the volatile configuration with id=1
{{ ydb-cli }} admin volatile-config fetch --id 1
# Apply the volatile configuration volatile.yaml to the cluster
{{ ydb-cli }} admin volatile-config add -f volatile.yaml
# Delete volatile configurations with id=1 and id=3 on the cluster
{{ ydb-cli }} admin volatile-config drop --id 1 --id 3
# Delete all volatile configurations on the cluster
{{ ydb-cli }} admin volatile-config drop --all
```

## Example of working with volatile configuration

Temporarily enabling logging settings for the `blobstorage` component to `DEBUG` on the node `host1.example.com`:
```bash
# Request current metadata to form a correct header for the volatile configuration
$ {{ ydb-cli }} admin config fetch --all
---
kind: MainConfig
cluster: "example-cluster-name"
version: 2
config:
  # ...
---
kind: VolatileConfig
cluster: "example-cluster-name"
version: 2
id: 1
selector_config:
  # ...
# Load configuration with version 2, cluster name example-cluster-name, and identifier 2
$ {{ ydb-cli }} admin volatile-config add -f - <<<EOF
metadata:
  kind: VolatileConfig
  cluster: "example-cluster-name"
  version: 2
  id: 2
selector_config:
- description: Set blobstorage logging level to DEBUG
  selector:
    node_host: host1.example.com
  config:
    log_config: !inherit
      entry: !inherit_key:component
      - component: BLOBSTORAGE
        level: 8
EOF
# ...
# log analysis
# ...
# Delete the configuration
$ {{ ydb-cli }} admin volatile-config drop --id 2
```