# Dynamic Cluster Configuration

Dynamic configuration allows running dynamic [nodes](../../concepts/cluster/common_scheme_ydb#nodes) by configuring them centrally without manually distributing files across the nodes. {{ ydb-short-name }} acts as a configuration management system, providing tools for reliable storage, versioning, and delivery of configurations, as well as a [DSL (Domain Specific Language)](./dynamic-config-selectors.md) for overriding parts of the configuration for specific groups of nodes. The configuration is a YAML document and is an extended version of the static configuration:

* The configuration description is moved to the `config` field
* The `metadata` field is added for validation and versioning
* The `allowed_labels` and `selector_config` fields are added for granular overrides of settings

This configuration is uploaded to the cluster, where it is reliably stored and delivered to each dynamic node upon startup. [Certain settings](#dynamic-kinds) are updated on the fly without restarting nodes. Using dynamic configuration, you can centrally solve the following tasks:

* Change logging levels for all or specific components across the entire cluster or for specific groups of nodes.
* Enable experimental features (feature flags) on specific databases.
* Change actor system settings on individual nodes or groups of nodes.

## Preparing to use the dynamic configuration {#preparation}

The following tasks should be performed before using the dynamic configuration in the cluster:

1. Enable [database node authentication and authorization](../../devops/deployment-options/manual/node-authorization.md).

2. Export the current settings from the [CMS](../../concepts/glossary.md#cms) in YAML format using the following command if [CMS-based configuration management](cms.md) has been used in the cluster:

    ```bash
    ./ydbd -s grpcs://<node1.ydb.tech>:2135 --ca-file ca.crt --token-file ydbd-token \
         admin console configs dump-yaml > dynconfig.yaml
    ```

    Before running the command shown above, obtain the authentication token using the `ydb auth get-token` command, as detailed in the [cluster initial deployment procedure](../../devops/deployment-options/manual/initial-deployment.md#initialize-cluster).

3. Prepare the initial dynamic configuration file:

   * If there are non-empty CMS settings exported in the previous step, adjust the YAML file with the exported CMS settings:
      * Add the `metadata` section based on the [configuration example](#example).
      * Add the `yaml_config_enabled: true` parameter to the `config` section.
   * If there are no previous CMS-based settings, use the [minimal configuration example](#example).
   * For clusters using TLS encryption for [actor system interconnect](../../concepts/glossary.md#actor-system-interconnect), add the [interconnect TLS settings](../../reference/configuration/tls.md#interconnect) to the `config` section.

4. Apply the dynamic configuration settings file to the cluster:

    ```bash
    # Apply the dynconfig.yaml on the cluster
    {{ ydb-cli }} admin config replace -f dynconfig.yaml
    ```

{% note info %}

The legacy configuration management via CMS will become unavailable after enabling dynamic configuration support on the {{ ydb-short-name }} cluster.

{% endnote %}

## Configuration examples {#example}

Example of a minimal dynamic configuration for a single-datacenter cluster:

```yaml
# Configuration metadata.
# This field is managed by the server.
metadata:
  # Cluster name from the cluster_uuid parameter set during cluster installation, or "", if the parameter is not set.
  cluster: ""
  # Configuration file identifier, always increments by 1 starting from 0.
  # Automatically increases when a new configuration is uploaded to the server.
  version: 0
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
    cpu_count: 14
allowed_labels: {}
selector_config: []
```

Detailed configuration parameters are described on the [{#T}](../../reference/configuration/index.md) page.

By default, the cluster configuration is assigned version 1. When applying a new configuration, the system compares the uploaded configuration's version with the value specified in the YAML file. If the versions match, the current version number is automatically incremented by one.

Below is a more comprehensive example of a dynamic configuration that defines typical global parameters as well as parameters specific to a particular database:

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

## Updating the dynamic configuration

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

## Operation Mechanism

### Configuration Update from the Administrator's Perspective

1. The configuration file is uploaded by the user using a [grpc call](https://github.com/ydb-platform/ydb/blob/5251c9ace0a7617c25d50f1aa4d0f13e3d56f985/ydb/public/api/grpc/draft/ydb_dynamic_config_v1.proto#L22) or [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md) to the cluster.
2. The file is checked for validity, basic constraints, version correctness, cluster name correctness, and the correctness of the configurations obtained after DSL transformation are verified.
3. The configuration version in the file is incremented by one.
4. The file is reliably stored in the cluster using the Console [tablet](../../concepts/glossary.md#tablet).
5. File updates are distributed across the cluster nodes.

### Configuration Update from the Cluster Node's perspective

1. Each node requests the entire configuration at startup.
2. Upon receiving the configuration, the node [generates the final configuration](./dynamic-config-selectors.md#selectors-resolve) for its set of [labels](./dynamic-config-selectors.md#selectors-intro).
3. The node subscribes to configuration updates by registering with the Console tablet.
4. In case of configuration updates, the local service receives it and transforms it for the node's labels.
5. All local services subscribed to updates receive the updated configuration.

Steps 1 and 2 are performed only for dynamic cluster nodes.

### Configuration Versioning

This mechanism prevents concurrent configuration modifications and makes updates idempotent. When a modification request is received, the server compares the version of the received modification with the stored one. If the version is one less, the configurations are compared: if they are identical, it means the user is attempting to upload the configuration again, the user receives OK, and the cluster configuration is not updated. If the version matches the current one on the cluster, the configuration is replaced with the new one, and the version field is incremented by one. In all other cases, the user receives an error.

## Dynamically Updated Settings {#dynamic-kinds}

Some system settings are updated without restarting nodes. To change them, upload a new configuration and wait for it to propagate across the cluster.

List of dynamically updated settings:

* `immediate_controls_config`
* `log_config`
* `memory_controller_config`
* `monitoring_config`
* `table_service_config`
* `tracing_config.external_throttling`
* `tracing_config.sampling`

The list may be expanded in the future.

## Limitations

* Using more than 30 different [labels](./dynamic-config-selectors.md) in [selectors](./dynamic-config-selectors.md) can lead to validation delays of several seconds, as {{ ydb-short-name }} needs to check the validity of each possible final configuration. The number of values for a single label has much less impact.
* Using large files (more than 500KiB for a cluster with 1000 nodes) can lead to increased network traffic in the cluster when updating the configuration. The traffic volume is directly proportional to the number of nodes and the configuration size.

