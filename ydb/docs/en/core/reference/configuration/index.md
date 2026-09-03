# Cluster configuration parameters

The cluster configuration is specified in a YAML file that is passed in the `--yaml-config` parameter when starting cluster nodes. This article describes the main configuration sections and provides links to detailed documentation for each section.

Each configuration section serves a specific purpose in setting up the {{ ydb-short-name }} cluster, from hardware resource allocation to security settings and feature flags. The configuration is organized into logical groups corresponding to various aspects of cluster management and operation.

## Configuration sections

The following configuration sections are available, listed in alphabetical order:

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
| [{#T}](monitoring_config.md) | No | Parameters of [YDB Monitoring](../ydb-ui/ydb-monitoring.md) |
| [{#T}](node_broker_config.md) | No | Stable node name configuration |
| [{#T}](query_service_config.md) | No | Configuration of external sources for federated queries |
| [{#T}](resource_broker_config.md) | No | Resource broker for controlling CPU and memory consumption |
| [{#T}](security_config.md) | No | Security configuration settings |
| [{#T}](self_management_config.md) | No | Distributed configuration V2 and automatic cluster component management settings |
| [{#T}](system_tablet_backup_config.md) | No | System tablet backup configuration |
| [{#T}](table_service_config.md) | No | Query execution configuration settings |
| [{#T}](tli_config.md) | No | Diagnostics parameters for [transaction lock invalidation](../../concepts/glossary.md#tli) (TLI) |
| [{#T}](tls.md) | No | TLS configuration for secure connections |

## Practical recommendations

This documentation section provides a complete description of available settings, while practical recommendations on what and when to configure can be found in the following places:

- As part of the initial {{ ydb-short-name }} cluster deployment:
- [Ansible](../../devops/deployment-options/ansible/initial-deployment/index.md)
- [Kubernetes](../../devops/deployment-options/kubernetes/initial-deployment.md)
- [Manually](../../devops/deployment-options/manual/initial-deployment/index.md)
- As part of [troubleshooting](../../troubleshooting/index.md)
- As part of [security hardening](../../security/index.md)

## Cluster configuration examples

Sample cluster configurations for deployment can be found in the [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Review them before deploying the cluster.
