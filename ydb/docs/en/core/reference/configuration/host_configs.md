# host_configs

A {{ ydb-short-name }} cluster consists of multiple nodes, and one or more typical server configurations are usually used for their deployment. To avoid repeating their description for each node, there is a `host_configs` section in the configuration file that lists the used configurations and assigned IDs.

## Syntax

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: <path_to_device>
    type: <type>
    disk_scope: <disk_scope>  # optional attribute
  - path: ...
- host_config_id: 2
  ...
```

The `host_config_id` attribute specifies a numeric configuration ID. The `drive` attribute contains a collection of descriptions of connected drives. Each description consists of two mandatory attributes:

- `path`: Path to the mounted block device, for example, `/dev/disk/by-partlabel/ydb_disk_ssd_01`
- `type`: Type of the device's physical media: `ssd`, `nvme`, or `rot` (rotational - HDD)

Additionally, an optional `disk_scope` attribute can be specified — a label of the device's failure zone within a node, see [Configuring DiskScope](#disk-scope).

## Examples

One configuration with ID 1 and one SSD disk accessible via `/dev/disk/by-partlabel/ydb_disk_ssd_01`:

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
```

Two configurations with IDs 1 (two SSD disks) and 2 (three SSD disks):

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
- host_config_id: 2
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    type: SSD
```

## Configuring DiskScope {#disk-scope}

`disk_scope` is an optional string attribute of a disk that defines a finer-grained failure zone within a single node. It is taken into account when calculating [fail domains](../../concepts/glossary.md#fail-domain) during the selection of disks for placing [VDisks](../../concepts/glossary.md#vdisk) of [storage groups](../../concepts/glossary.md#storage-group).

A fail domain is determined by the physical location of a disk (data center, rack, server, physical device), so either no two VDisks of the same group can be placed on the same node (if the fail domain corresponds to a server or a server rack), or several VDisks of the same group can be placed on the same node on different disks (if the fail domain corresponds to an individual physical device). In the first case, it is impossible to create a configuration in `block-4-2` mode with fewer than 8 storage nodes, and in the second case the configuration might not stand the failure of a single node. To limit the number of VDisks of the same group placed on a single node, physical devices can be labeled with the `disk_scope` attribute, and fail domain calculation can be enabled at the `disk_scope` level. This makes it possible to build some fault-tolerant configurations in installations that have fewer servers than the number of fail domains required by the chosen [fault tolerance mode](../../concepts/topology.md).

### Example {#disk-scope-example}

A cluster of 4 servers with 4 disks on each server, in the `block-4-2` fault tolerance mode:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
    disk_scope: fail-domain-1
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_02
    type: SSD
    disk_scope: fail-domain-1
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_03
    type: SSD
    disk_scope: fail-domain-2
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_04
    type: SSD
    disk_scope: fail-domain-2
```

To calculate fail domains at the `disk_scope` level for the static group and other storage groups, specify the corresponding fail domain type in the yaml config (top-level key):

``` yaml
fail_domain_type: disk_scope
```

And in the domain configuration:

```yaml
domains:
- domain_name: <domain name>
  ...
  storage_pool_kinds:
  - kind: <type of physical devices used>
    fail_domain_type: disk_scope
    ...
```

With this configuration, two VDisks of each storage group will be placed on each server, and the maximum tolerated failure in such a configuration is the failure of a single server.

## Kubernetes Features {#host-configs-k8s}

The {{ ydb-short-name }} Kubernetes operator mounts NBS disks for Storage nodes at the path `/dev/kikimr_ssd_00`. To use them, the following `host_configs` configuration must be specified:

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

The example configuration files provided with the {{ ydb-short-name }} Kubernetes operator contain this section, and it does not need to be changed.