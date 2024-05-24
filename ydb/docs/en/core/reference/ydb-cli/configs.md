## Working with configuration

### Common command flags

* `-f, --filename <filename.yaml>` — read input from a file, `-` for STDIN. It can be specified multiple times for commands accepting multiple files (e.g., resolve). The metadata field will determine the file type.
* `--output-directory <dir>` — dump/resolve files to a directory.
* `--strip-metadata` — remove the metadata field from the output.
* `--all` — extend command output to the entire configuration (see advanced configuration).
* `--allow-unknown-fields` — allow ignoring unknown fields in the configuration.

```bash
# Apply the configuration dynconfig.yaml to the cluster
{{ ydb-cli }} admin config replace -f dynconfig.yaml
# Check if it is possible to apply the configuration dynconfig.yaml to the cluster (validate all validators, version, and cluster match)
{{ ydb-cli }} admin config replace -f dynconfig.yaml --dry-run
# Apply the configuration dynconfig.yaml to the cluster, ignoring version and cluster checks (version and cluster will still be overwritten with correct ones)
{{ ydb-cli }} admin config replace -f dynconfig.yaml --force
# Fetch the main cluster configuration
{{ ydb-cli }} admin config fetch
# Fetch all current configuration files of the cluster
{{ ydb-cli }} admin config fetch --all
# Generate all possible final configurations for dynconfig.yaml
{{ ydb-cli }} admin config resolve --all -f dynconfig.yaml
# Generate the final configuration for dynconfig.yaml with labels tenant=/Root/test and canary=true
{{ ydb-cli }} admin config resolve -f dynconfig.yaml --label tenant=/Root/test --label canary=true
# Generate the final configuration for dynconfig.yaml for labels from node 1003
{{ ydb-cli }} admin config resolve -f dynconfig.yaml --node-id 1003
# Fetch all temporary configurations of the cluster
{{ ydb-cli }} admin volatile-config fetch --all --output-directory <dir>
# Fetch the temporary configuration with id 1 from the cluster
{{ ydb-cli }} admin volatile-config fetch --id 1
# Apply the temporary configuration volatile.yaml to the cluster
{{ ydb-cli }} admin volatile-config add -f volatile.yaml
# Delete temporary configurations with ids 1 and 3 on the cluster
{{ ydb-cli }} admin volatile-config drop --id 1 --id 3
# Delete all temporary configurations on the cluster
{{ ydb-cli }} admin volatile-config drop --all
```

## Scenarios

### Update the main cluster configuration
 ```bash
# Fetch the cluster configuration
{{ ydb-cli }} admin config fetch > dynconfig.yaml
# Edit the configuration with your favorite editor
vim dynconfig.yaml
# Apply the configuration dynconfig.yaml to the cluster
{{ ydb-cli }} admin config replace -f dynconfig.yaml
```
Similarly, in one line:
```bash
{{ ydb-cli }} admin config fetch | yq '.config.actor_system_config.scheduler.resolution = 128' | {{ ydb-cli }} admin config replace -f -
```
Command output:
```
OK
```
### View the configuration for a specific set of labels
```bash
{{ ydb-cli }} admin config resolve --remote --label tenant=/Root/db1 --label canary=true
```
Command output:
```yaml
---
label_sets:
- dynamic:
    type: COMMON
    value: true
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
```

### View the configuration for a specific node
```bash
{{ ydb-cli }} admin config resolve --remote --node-id <node_id>
```
Command output:
```yaml
---
label_sets:
- dynamic:
    type: COMMON
    value: true
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
```

### Save all configurations locally
```bash
{{ ydb-cli }} admin config fetch --all --output-directory <configs_dir>
ls <configs_dir>
```
Command output:
```
dynconfig.yaml volatile_1.yaml volatile_3.yaml
```

### View all configurations locally
```bash
{{ ydb-cli }} admin config fetch --all
```
Command output:
```yaml
---
metadata:
  kind: main
  cluster: unknown
  version: 1
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
allowed_labels: {}
selector_config: []
---
metadata:
  kind: volatile
  cluster: unknown
  version: 1
  id: 1
# some comment example
selectors:
- description: test
  selector:
    tenant: /Root/db1
  config:
    actor_system_config: !inherit
      use_auto_config: true
      cpu_count: 12
```

### View the final configuration for a specific node from the locally saved original configuration
```bash
{{ ydb-cli }} admin config resolve -k <configs_dir> --node-id <node_id>
```
Command output:
```yaml
---
label_sets:
- dynamic:
    type: COMMON
    value: true
config:
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 4
```