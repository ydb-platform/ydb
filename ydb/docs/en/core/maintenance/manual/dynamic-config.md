## Dynamic cluster configuration

Dynamic configuration allows running dynamic [nodes](../../concepts/cluster/common_scheme_ydb#nodes) by configuring them centrally without manually distributing files across the nodes. {{ ydb-short-name }} acts as a configuration management system, providing tools for reliable storage, versioning, and delivery of configurations, as well as a [DSL (Domain Specific Language)](./dynamic-config-selectors.md) for overriding parts of the configuration for specific groups of nodes. The configuration is a YAML document and is an extended version of the static configuration:

* The configuration description is moved to the `config` field
* The `metadata` field is added for validation and versioning
* The `allowed_labels` and `selector_config` fields are added for granular overrides of settings

This configuration is uploaded to the cluster, where it is reliably stored and delivered to each dynamic node upon startup. [Certain settings](#dynamic-kinds) are updated on the fly without restarting nodes. Using dynamic configuration, you can centrally solve the following tasks:
* Switch logging settings for components for the entire cluster or specific groups of nodes;
* Enable experimental features (feature flags) on specific databases;
* Change actor system settings on individual nodes or groups of nodes;
* And more.

### Configuration examples {#example}

Example of a minimal dynamic configuration for a single-datacenter cluster:
```yaml
# Configuration metadata.
# This field is managed by the server.
metadata:
 # Cluster name from the cluster_uuid parameter set during cluster installation, or "", if the parameter is not set.
 cluster: unknown
 # Configuration file identifier, always increments by 1 starting from 1.
 # Automatically increases when a new configuration is uploaded to the server.
 version: 1
# Main cluster configuration. All values here are applied by default unless overridden by selectors.
# Content is similar to the static cluster configuration.
config:
# It must always be set to true when using YAML configuration.
 yaml_config_enabled: true
# Actor system configuration, as by default, this section is used only by dynamic nodes.
# Configuration is set specifically for them.
 actor_system_config:
# Automatic configuration selection for the node based on type and available cores.
 use_auto_config: true
# HYBRID || COMPUTE || STORAGE — node type.
 node_type: COMPUTE
# Number of cores.
 cpu_count: 4
# Root domain configuration of the {{ ydb-short-name }} cluster.
 domains_config:
 domain:
 - name: Root
 storage_pool_types:
 - kind: ssd
 pool_config:
 box_id: 1
 erasure_species: none
 kind: ssd
 pdisk_filter:
 - property:
 - type: SSD
 vdisk_kind: Default
 explicit_mediators: [72075186232426497, 72075186232426498, 72075186232426499]
 explicit_coordinators: [72075186232360961, 72075186232360962, 72075186232360963]
 explicit_allocators: [72075186232492033, 72075186232492034, 72075186232492035]
 state_storage:
 - ssid: 1
 ring:
 nto_select: 5
 node: [1, 2, 3, 4, 5, 6, 7, 8]
 hive_config:
 - hive_uid: 1
 hive: 72057594037968897
# Channel profile configuration for tablets.
 channel_profile_config:
 profile:
 - profile_id: 0
 channel:
 - &default_channel
 erasure_species: block-4-2
 pdisk_category: 1
 vdisk_category: Default
 - *default_channel
 - *default_channel
allowed_labels: {}
selector_config: []
```

Detailed configuration parameters are described on the [{#T}](../../deploy/configuration/config.md) page.

By default, the cluster configuration is empty and has version 1. When applying a new configuration, the uploaded configuration's version is compared and automatically incremented by one.

### Updating dynamic configuration

```bash
# Fetch the cluster configuration
{{ ydb-cli }} admin config fetch > dynconfig.yaml
# Edit using any text editor
vim dynconfig.yaml
# Apply the configuration file dynconfig.yaml to the cluster
{{ ydb-cli }} admin config replace -f dynconfig.yaml
```

Additional configuration options are described on the [selectors](./dynamic-config-selectors.md) and [temporary configuration](./dynamic-config-volatile-config.md) pages.
All commands for working with configuration are described in the [{#T}](../../reference/ydb-cli/configs.md) section.

### Operation mechanism

#### Configuration update from the administrator's perspective

1. The configuration file is uploaded by the user using a [grpc call](https://github.com/ydb-platform/ydb/blob/5251c9ace0a7617c25d50f1aa4d0f13e3d56f985/ydb/public/api/grpc/draft/ydb_dynamic_config_v1.proto#L22) or [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md) to the cluster.
2. The file is checked for validity, basic constraints, version correctness, cluster name correctness, and the correctness of the configurations obtained after DSL transformation are verified.
3. The configuration version in the file is incremented by one.
4. The file is reliably stored in the cluster using the Console [tablet](../../concepts/cluster/common_scheme_ydb.md#tablets).
5. File updates are distributed across the cluster nodes.

#### Configuration update from the cluster node's perspective

1. Each node requests the entire configuration at startup.
2. Upon receiving the configuration, the node [generates the final configuration](./dynamic-config-selectors.md#selectors-resolve) for its set of [labels](./dynamic-config-selectors.md#selectors-intro).
3. The node subscribes to configuration updates by registering with the Console tablet.
4. In case of configuration updates, the local service receives it and transforms it for the node's labels.
5. All local services subscribed to updates receive the updated configuration.

Steps 1 and 2 are performed only for dynamic cluster nodes.

#### Configuration versioning

This mechanism prevents concurrent configuration modifications and makes updates idempotent. When a modification request is received, the server compares the version of the received modification with the stored one. If the version is one less, the configurations are compared: if they are identical, it means the user is attempting to upload the configuration again, the user receives OK, and the cluster configuration is not updated. If the version matches the current one on the cluster, the configuration is replaced with the new one, and the version field is incremented by one. In all other cases, the user receives an error.

### Dynamically updated settings {#dynamic-kinds}

Some system settings are updated without restarting nodes. To change them, upload a new configuration and wait for it to propagate across the cluster.

List of dynamically updated settings:
* `log_config`
* `immediate_controls_config`
* `table_service_config`
* `monitoring_config`
* `tracing_config.sampling`
* `tracing_config.external_throttling`

The list may be expanded in the future.

### Limitations

* Using more than 30 different [labels](./dynamic-config-selectors.md) in [selectors](./dynamic-config-selectors.md) can lead to validation delays of several seconds, as {{ ydb-short-name }} needs to check the validity of each possible final configuration. The number of values for a single label has much less impact.
* Using large files (more than 500KiB for a cluster with 1000 nodes) can lead to increased network traffic in the cluster when updating the configuration. The traffic volume is directly proportional to the number of nodes and the configuration size.