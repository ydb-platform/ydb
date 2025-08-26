
# {{ ydb-short-name }} Cluster Configuration

The cluster configuration is specified in the YAML file passed in the `--yaml-config` parameter when the cluster nodes are run. This article provides an overview of the main configuration sections and links to detailed documentation for each section.

Each configuration section serves a specific purpose in defining how the {{ ydb-short-name }} cluster operates, from hardware resource allocation to security settings and feature flags. The configuration is organized into logical groups that correspond to different aspects of cluster management and operation.

## Configuration Sections

The following top-level configuration sections are available, listed in alphabetical order:

#|
|| **Section** | **Required** | **Description** ||
|| [{#T}](actor_system_config.md) | Yes | CPU resource allocation across actor system pools ||
|| [{#T}](auth_config.md) | No | Authentication and authorization settings ||
|| [{#T}](blob_storage_config.md) | No | Static cluster group configuration for system tablets ||
|| [{#T}](client_certificate_authorization.md) | No | Client certificate authentication ||
|| [{#T}](domains_config.md) | No | Cluster domain configuration including Blob Storage and State Storage ||
|| [{#T}](feature_flags.md) | No | Feature flags to enable or disable specific {{ ydb-short-name }} features ||
|| [{#T}](healthcheck_config.md) | No | Health check service thresholds and timeout settings ||
|| [{#T}](hive.md) | No | Hive component configuration for tablet management ||
|| [{#T}](host_configs.md) | No | Typical host configurations for cluster nodes ||
|| [{#T}](hosts.md) | Yes | Static cluster nodes configuration ||
|| [{#T}](log_config.md) | No | Logging configuration and parameters ||
|| [{#T}](memory_controller_config.md) | No | Memory allocation and limits for database components ||
|| [{#T}](node_broker_config.md) | No | Stable node names configuration ||
|| [{#T}](resource_broker_config.md) | No | Resource broker for controlling CPU and memory consumption ||
|| [{#T}](security_config.md) | No | Security configuration settings ||
|| [{#T}](table_service_config.md) | No | Query processing configuration||
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

