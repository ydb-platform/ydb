# Volatile Configurations

{% include [deprecated](_includes/deprecated.md) %}

Volatile configurations are a special type of configurations that complement dynamic ones and are not persistent. That is, these configurations are reset when the [Console tablet](../../../concepts/glossary.md#console) moves or restarts, as well as when the main configuration is updated.

Main usage scenarios:

- temporary configuration changes for debugging or testing;
- trial enabling of potentially dangerous settings. In case of cluster crash or restart, these settings will be automatically disabled.

These configurations are added to the end of the selector set, the description syntax is identical to [selector syntax](dynamic-config-selectors.md).

```bash
# Get all volatile configurations loaded on the cluster
{{ ydb-cli }} admin volatile-config fetch --all --output-directory <dir>
# Get volatile configuration with id=1
{{ ydb-cli }} admin volatile-config fetch --id 1
# Apply volatile configuration volatile.yaml to the cluster
{{ ydb-cli }} admin volatile-config add -f volatile.yaml
# Remove volatile configurations with id=1 and id=3 on the cluster
{{ ydb-cli }} admin volatile-config drop --id 1 --id 3
# Remove all volatile configurations on the cluster
{{ ydb-cli }} admin volatile-config drop --all
```

## Example of Working with Volatile Configuration

Temporarily enabling `blobstorage` component logging settings to `DEBUG` on node `host1.example.com`:

```bash
# Request current metadata to form correct volatile configuration header
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
# Load configuration with version 2, cluster name example-cluster-name and identifier 2
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
# Remove configuration
$ {{ ydb-cli }} admin volatile-config drop --id 2
```