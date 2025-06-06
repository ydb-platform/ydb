# Dynamic Cluster Configuration

{% include [deprecated](_includes/deprecated.md) %}

Dynamic configuration allows running dynamic [nodes](../../../concepts/glossary.md#node) by configuring them centrally, without the need to manually distribute files across nodes. {{ ydb-short-name }} acts as a configuration management system, providing tools for reliable storage, versioning, and configuration delivery, as well as a [DSL (Domain Specific Language)](dynamic-config-selectors.md) for overriding parts of it for specific node groups. The configuration is a YAML document. It is an extended version of static configuration:

* configuration description is moved to the `config` field
* a `metadata` field is added, necessary for validation and versioning
* `allowed_labels` and `selector_config` fields are added for granular setting overrides

This configuration is loaded into the cluster, where it is reliably stored and delivered to each dynamic node at startup. [Some settings](#dynamic-kinds) are updated on the fly, without restarting nodes. Using dynamic configuration, you can centrally solve the following tasks:

* switch component logging settings for both the entire cluster and individual databases or node groups
* enable experimental functionality (feature flags) on individual databases
* change actor system settings on a specific database, individual node, or group of nodes

## Preparing to Use Dynamic Configuration {#preparation}

Before starting to use dynamic configuration in the cluster, it is necessary to perform preparatory work:

1. Enable [database node authentication and authorization](node-authorization.md) in the cluster.

2. If [configuration management via CMS](cms.md) was previously used in the cluster, export existing settings in YAML format. To do this, run the following command:

    ```bash
    ./ydbd -s grpcs://<node1.ydb.tech>:2135 --ca-file ca.crt --token-file ydbd-token \
         admin console configs dump-yaml > dynconfig.yaml
    ```

    You must first obtain an authentication token using the `ydb auth get-token` command, similar to the [cluster initialization procedure](../../../devops/deployment-options/manual/initial-deployment.md#initialize-cluster).

3. Create the initial dynamic configuration file:

   * If settings were previously made in CMS (exported in the previous step), use the resulting file as a basis and:
      * add a `metadata` section following the pattern from the [configuration example](#example)
      * add the `yaml_config_enabled: true` parameter to the `config` section
   * If no settings were previously made via CMS, use the [minimal content](#example) of the dynamic configuration file.
   * If the cluster uses [actor system interconnect](../../../concepts/glossary.md#actor-system-interconnect) encryption, add the corresponding [TLS settings for interconnect](../../../reference/configuration/tls.md#interconnect) to the `config` section.

4. Apply the created dynamic configuration file to the cluster:

    ```bash
    # Apply configuration file dynconfig.yaml to the cluster
    {{ ydb-cli }} admin config replace -f dynconfig.yaml
    ```

{% note info %}

After enabling dynamic configuration support in the {{ ydb-short-name }} cluster, the legacy configuration management function via CMS will become unavailable.

{% endnote %}

## Configuration examples {#example}

Example of minimal dynamic configuration for a single-datacenter cluster:

```yaml
# Configuration metadata.
# Field managed by server
metadata:
  # Cluster name from cluster_uuid parameter set during cluster installation, or "" if parameter is not set
  cluster: ""
  # Configuration file identifier, always increases by 1 and starts from 0.
  # Automatically increased when loading new configuration to server.
  version: 0
# Main cluster configuration, all values from it are applied by default until overridden by selectors.
# Content is similar to static cluster configuration
config:
  # must always be set to true to use yaml configuration
  yaml_config_enabled: true
  # actor system configuration - since by default this section is used
  # only by DB nodes, configuration is set specifically for them
  actor_system_config:
    # automatic configuration selection for node based on type and number of available cores
    use_auto_config: true
    # HYBRID || COMPUTE || STORAGE — node type, always COMPUTE for database nodes
    node_type: COMPUTE
    # number of allocated cores
    cpu_count: 14
allowed_labels: {}
selector_config: []
```

Configuration parameters are described in more detail on the [{#T}](../../../reference/configuration/index.md) page.

The initially installed cluster dynamic configuration gets version number 1. When applying a new configuration, the version of the stored configuration is compared with that specified in the YAML document and automatically increased by one.

Example of more complex dynamic configuration with typical global parameters and special parameters for one of the databases:

```yaml
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
config:
  yaml_config_enabled: true
  table_profiles_config:
    table_profiles:
    - name: default
      compaction_policy: default
      execution_policy: default
      partitioning_policy: default
      storage_policy: default
      replication_policy: default
      caching_policy: default
    compaction_policies:
    - name: default
    execution_policies:
    - name: default
    partitioning_policies:
    - name: default
      auto_split: true
      auto_merge: true
      size_to_split: 2147483648
    storage_policies:
    - name: default
      column_families:
      - storage_config:
          sys_log:
            preferred_pool_kind: ssd
          log:
            preferred_pool_kind: ssd
          data:
            preferred_pool_kind: ssd
    replication_policies:
    - name: default
    caching_policies:
    - name: default
  interconnect_config:
    encryption_mode: REQUIRED
    path_to_certificate_file: "/opt/ydb/certs/node.crt"
    path_to_private_key_file: "/opt/ydb/certs/node.key"
    path_to_ca_file: "/opt/ydb/certs/ca.crt"
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: Custom settings for testdb
  selector:
    tenant: /cluster1/testdb
  config:
    shared_cache_config:
      memory_limit: 34359738368
    feature_flags: !inherit
      enable_views: true
    actor_system_config:
      use_auto_config: true
      node_type: COMPUTE
      cpu_count: 14
```

## Updating Dynamic Configuration

```bash
# Get cluster configuration
{{ ydb-cli }} admin config fetch > dynconfig.yaml
# Edit using any text editor
vim dynconfig.yaml
# Apply configuration file dynconfig.yaml to cluster
{{ ydb-cli }} admin config replace -f dynconfig.yaml
```

Additional configuration capabilities are described on the [selectors](dynamic-config-selectors.md) and [volatile configuration](dynamic-config-volatile-config.md) pages.
All commands for working with configuration are described in the [{#T}](../../../reference/ydb-cli/configs.md) section.

## Implementation

### Configuration Update from Administrator Perspective

1. The configuration file is loaded by the user using a [grpc call](https://github.com/ydb-platform/ydb/blob/5251c9ace0a7617c25d50f1aa4d0f13e3d56f985/ydb/public/api/grpc/draft/ydb_dynamic_config_v1.proto#L22) or [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md) into the cluster.
2. The file is checked for validity, basic constraints are checked, version correctness, cluster name correctness, correctness of configurations obtained after DSL transformation.
3. The configuration version in the file is increased by one.
4. The file is reliably stored in the cluster by the [Console tablet](../../../concepts/glossary.md#console).
5. File updates are distributed across cluster nodes.

### Configuration Update from Cluster Node Perspective

1. Each node requests full configuration at startup.
2. Upon receiving configuration, the node [generates final configuration](dynamic-config-selectors.md#selectors-resolve) for its set of [labels](dynamic-config-selectors.md#selectors-intro).
3. The node subscribes to configuration updates by registering with the [Console tablet](../../../concepts/glossary.md#console).
4. In case of configuration update, the local service receives it and transforms it for the node's labels.
5. All local services subscribed to updates receive the updated configuration.

Points 1,2 are performed only for dynamic cluster nodes.

### Configuration Versioning

This mechanism allows avoiding concurrent configuration modification and making its update idempotent. When receiving a configuration modification request, the server compares the version of the received modification with the saved one. If the version is one less, then configurations are compared — if they are identical, it means the user is trying to load the configuration again, the user gets OK, and the configuration on the cluster is not updated. If the version equals the current one on the cluster, then the configuration is replaced with the new one, while the version field is increased by one. In all other cases, the user gets an error.

## Dynamically Updatable Settings {#dynamic-kinds}

Part of the system settings are updated without restarting nodes. To change them, it is sufficient to load a new configuration and wait for its distribution across the cluster.

List of dynamically updatable settings:

* `immediate_controls_config`
* `log_config`
* `memory_controller_config`
* `monitoring_config`
* `table_service_config`
* `tracing_config.external_throttling`
* `tracing_config.sampling`

The list may be expanded in the future.

## Limitations

* Using more than 30 different [labels](dynamic-config-selectors.md) in [selectors](dynamic-config-selectors.md) can lead to delays of tens of seconds during configuration validation, as {{ ydb-short-name }} needs to check the validity of each possible final configuration. At the same time, the number of values of one label has much less impact.
* Using large files (more than 500KiB for a 1000-node cluster) configuration can lead to increased network traffic in the cluster when updating configuration. Traffic volume is directly proportional to the number of nodes and configuration volume.