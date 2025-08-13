# {{ ydb-short-name }} Cluster Configuration

The cluster configuration is specified in the YAML file passed in the `--yaml-config` parameter when the cluster nodes are run. This article provides an overview of the main configuration sections and links to detailed documentation for each section.

Each configuration section serves a specific purpose in defining how the {{ ydb-short-name }} cluster operates, from hardware resource allocation to security settings and feature flags. The configuration is organized into logical groups that correspond to different aspects of cluster management and operation.

## Configuration Sections

The following top-level configuration sections are available, listed in alphabetical order:

- [{#T}](actor_system_config.md) — CPU resource allocation across actor system pools
- [{#T}](auth_config.md) — Authentication and authorization settings
- [{#T}](blob_storage_config.md) — Static cluster group configuration for system tablets
- [{#T}](client_certificate_authorization.md) — Client certificate authentication
- [{#T}](domains_config.md) — Cluster domain configuration including Blob Storage and State Storage
- [{#T}](feature_flags.md) — Feature flags to enable or disable specific {{ ydb-short-name }} features
- [{#T}](healthcheck_config.md) — Health check service thresholds and timeout settings
- [{#T}](hive.md) — Hive component configuration for tablet management
- [{#T}](host_configs.md) — Typical host configurations for cluster nodes
- [{#T}](hosts.md) — Static cluster nodes configuration
- [{#T}](log_config.md) — Logging configuration and parameters
- [{#T}](memory_controller_config.md) — Memory allocation and limits for database components
- [{#T}](node_broker_config.md) — Stable node names configuration
- [{#T}](resource_broker_config.md) — Resource broker for controlling CPU and memory consumption
- [{#T}](security_config.md) — Security configuration settings
- [{#T}](tls.md) — TLS configuration for secure connections

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
