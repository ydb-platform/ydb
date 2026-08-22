
# {{ ydb-short-name }} Cluster Configuration

The cluster configuration is specified in the YAML file passed in the `--yaml-config` parameter when the cluster nodes are run. This article provides an overview of the main configuration sections and links to detailed documentation for each section.

Each configuration section serves a specific purpose in defining how the {{ ydb-short-name }} cluster operates, from hardware resource allocation to security settings and feature flags. The configuration is organized into logical groups that correspond to different aspects of cluster management and operation.

## Configuration Sections

<<<<<<< HEAD
The following top-level configuration sections are available, listed in alphabetical order:
=======
| **Section** | **Required** | **Description** |
| --- | --- | --- |
| [{#T}](actor_system_config.md) | Yes | Distribution of CPU resources across actor system pools |
| [{#T}](auth_config.md) | No | Authentication and authorization settings |
| [{#T}](aws_client_config.md) | No | AWS client default settings |
| [{#T}](blob_storage_config.md) | No | Static cluster group configuration for system tablets |
| [{#T}](bridge_config.md) | No | Configuration of the [bridge mode](../../concepts/bridge.md) |
| [{#T}](client_certificate_authorization.md) | No | Authentication using client certificates |
| [{#T}](cms_config.md) | No | Cluster Management System (CMS) configuration |
| [{#T}](domains_config.md) | No | Cluster domain configuration, including Blob Storage and State Storage |
| [{#T}](feature_flags.md) | No | Feature flags to enable or disable certain {{ ydb-short-name }} capabilities |
| [{#T}](healthcheck_config.md) | No | Thresholds and timeouts for the Health Check service |
| [{#T}](hive_config.md) | No | Tablet launch configuration |
| [{#T}](host_configs.md) | No | Typical host configurations for cluster nodes |
| [{#T}](hosts.md) | Yes | Static cluster node configuration |
| [{#T}](immediate_controls_config.md) | No | Configuration of dynamic cluster settings |
| [{#T}](kafka_proxy_config.md) | No | Configuration of [Kafka Proxy](../../reference/kafka-api/index.md) |
| [{#T}](log_config.md) | No | Logging configuration and parameters |
| [{#T}](memory_controller_config.md) | No | Memory allocation and limits for database components |
| [{#T}](monitoring_config.md) | No | Parameters of [YDB Monitoring](../embedded-ui/ydb-monitoring.md) |
| [{#T}](node_broker_config.md) | No | Stable node name configuration |
| [{#T}](query_service_config.md) | No | Configuration of external sources for federated queries |
| [{#T}](resource_broker_config.md) | No | Resource broker for controlling CPU and memory consumption |
| [{#T}](security_config.md) | No | Security configuration settings |
| [{#T}](self_management_config.md) | No | Distributed configuration V2 and automatic cluster component management settings |
| [{#T}](system_tablet_backup_config.md) | No | System tablet backup configuration |
| [{#T}](table_service_config.md) | No | Query execution configuration settings |
| [{#T}](tli_config.md) | No | Diagnostics parameters for [transaction lock invalidation](../../concepts/glossary.md#tli) (TLI) |
| [{#T}](tls.md) | No | TLS configuration for secure connections |
>>>>>>> 617b68b36c5 (add description for force_shard_split_data_size setting (#41348))

#|
|| **Section** | **Required** | **Description** ||
|| [{#T}](actor_system_config.md) | Yes | CPU resource allocation across actor system pools ||
|| [{#T}](auth_config.md) | No | Authentication and authorization settings ||
|| [{#T}](blob_storage_config.md) | No | Static cluster group configuration for system tablets ||
|| [{#T}](bridge_config.md) | No | Cluster piles for bridge mode ||
|| [{#T}](client_certificate_authorization.md) | No | Client certificate authentication ||
|| [{#T}](cms_config.md) | No | Cluster Management System configuration ||
|| [{#T}](domains_config.md) | No | Cluster domain configuration including Blob Storage and State Storage ||
|| [{#T}](feature_flags.md) | No | Feature flags to enable or disable specific {{ ydb-short-name }} features ||
|| [{#T}](healthcheck_config.md) | No | Health check service thresholds and timeout settings ||
|| [{#T}](hive.md) | No | Hive component configuration for tablet management ||
|| [{#T}](host_configs.md) | No | Typical host configurations for cluster nodes ||
|| [{#T}](hosts.md) | Yes | Static cluster nodes configuration ||
|| [{#T}](kafka.md) | No | [Kafka Proxy](../../reference/kafka-api/index.md) configuration ||
|| [{#T}](log_config.md) | No | Logging configuration and parameters ||
|| [{#T}](memory_controller_config.md) | No | Memory allocation and limits for database components ||
|| [{#T}](node_broker_config.md) | No | Stable node names configuration ||
|| [{#T}](query_service_config.md) | No | Federated query connector configuration ||
|| [{#T}](resource_broker_config.md) | No | Resource broker for controlling CPU and memory consumption ||
|| [{#T}](security_config.md) | No | Security configuration settings ||
|| [{#T}](table_service_config.md) | No | Query processing configuration||
|| [{#T}](tli_config.md) | No | [Transaction lock invalidation](../../concepts/glossary.md#tli) (TLI) diagnostics parameters ||
|| [{#T}](tls.md) | No | TLS configuration for secure connections ||
|#

## Practical Guidelines

While this documentation section focuses on complete reference documentation for available settings, practical recommendations on what to tune and when can be found in the following places:

- As part of the initial {{ ydb-short-name }} cluster deployment:

    - [Ansible](../../devops/deployment-options/ansible/initial-deployment.md)
    - [Kubernetes](../../devops/deployment-options/kubernetes/initial-deployment.md)
    - [Manual](../../devops/deployment-options/manual/initial-deployment.md)

- As part of [troubleshooting](../../troubleshooting/index.md)
- As part of [security hardening](../../security/index.md)

## Sample Cluster Configurations

You can find model cluster configurations for deployment in the [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Check them out before deploying a cluster.

