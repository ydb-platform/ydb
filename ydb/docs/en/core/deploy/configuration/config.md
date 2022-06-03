# Cluster configuration

A cluster configuration is in a YAML-file, provided in a `--yaml-config` option when starting cluster nodes.

This section describes basic configration parameter groups of that file.

## host_configs - Host configuration templates {#host-configs}

YDB cluster comprise number of nodes typically deployed over one or more standard server configurations. To avoid duplication of its descriptions for each node, the configuration file containts a `host_configs` section where configuration templates are listed and assigned an identifier.

**Syntax**

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: <path_to_device>
    type: <type>
  - path: ...
- host_config_id: 2
  ...  
```

The `host_config_id` attribute sets a numeric template identifier. The `drive` attribute contains a collection of attached disks descriptions. Each  description contains two attributes:

- `path` : Path to a mounted block device, for instance `/dev/disk/by-partlabel/ydb_disk_ssd_01`
- `type` : Device physical media type: `ssd`, `nvme` or `rot` (rotational - HDD)

**Examples**

A sole template identified as 1, with a single disk of SSD type, available on `/dev/disk/by-partlabel/ydb_disk_ssd_01` path:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
```

Two templates identified as 1 and 2, with two and three SSD disks, respectively:

``` yaml
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

### Kubernetes specifics {#host-configs-k8s}

A YDB Kubernetes operator mounts NBS disks for storage nodes on the `/dev/kikimr_ssd_00` path. A following template must be specified in the `host_configs` to use them:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

Such section is already included in the configuration example files supplied with the YDB Kubernetes operator, and may not be changed.

## hosts - Cluster static nodes {#hosts}

This group contains a list of cluster static nodes where storage processes are running, and sets its attributes:

- Numeric node identifier
- DNS hostname and port where connection over IP network can be estalished
- [Host configuration template](#host-configs) identifier 
- Placement in a particular rack and availability zone
- Server serial number (optional)

**Syntax**

``` yaml
hosts:
- host: <DNS hostname>
  host_config_id: <numeric host configuration template identifier>
  port: <port> # 19001 by default
  walle_location:
    body: <string representing a server serial number>
    data_center: <string representing an availability zone identifier>
    rack: <string representing a rack identifier>
- host: <DNS hostname>
  ...
```

**Examples**

``` yaml
hosts:
- host: hostname1
  host_config_id: 1
  node_id: 1
  port: 19001
  walle_location:
    body: '1'
    data_center: '1'
    rack: '1'
- host: hostname2
  host_config_id: 1
  node_id: 2
  port: 19001
  walle_location:
    body: '1'
    data_center: '1'
    rack: '1'
```

### Kubernetes specifics {#hosts-k8s}

The whole section `hosts` content is generated automatically when using YDB Kubernetes operator, overwriting any user content provided as input to an operator call. All storage nodes use `host_config_id` = `1`, for which a correct [configuration template](#host-configs-k8s) must be provided.

## domains_config - Cluster domain {#domains-config}

This sections contains root domain configuration for the YDB cluster, including Blob Storage and State Storage configurations.

**Syntax**
``` yaml
domains_config:
  domain:
  - name: <root domain name>
    storage_pool_types: <Blob Storage configuration>
  state_storage: <State Storage configuration>
```

There can be only one root domain in a cluster, named as specified in the `domains_config.domain.name` attribute. Default configurations use the `Root` domain name.

### Blob Storage configuration {#domains-blob}

This sections defines one or more storage pool types available in the cluster for data in the databases, with the following configuration options:

- Storage pool name
- Device properties (e.g. type of disks)
- Data encryption (on/off)
- Fault tolerance mode

There are three fault tolerance modes currently supported:

|Mode name|Storage<br>overhead|Min number<br>of nodes|Description|
|---------|-|-----------|-|
| none | 1 | 1 | No data redundancy, any hardware fault leads to the unavailability of the storage pool. This mode may be useful for functional testing. |
| block-4-2 | 1.5 | 8 | [Erasure coding](https://en.wikipedia.org/wiki/Erasure_code) is applied with 2 redundancy blocks added to 4 chunks of original data. Storage nodes are assigned to at least 8 fail domains (usually racks). Storage pool is available when any of 2 domains are lost, writing all 6 parts of data to the remaining domains. This mode is applicable for storage pools within a single availability zone (usually a data center). |
| mirror-3-dc | 3 | 9 | Data is replicated in 3 availability zones using 3 fail domains (usually racks) inside each zone. Storage pool is available upon a failure of any availability zone and one failure domain in any of the remaining zones. |

Storage overhead shown above is for the fault tolerance factor only. When planning capacity, additional significant factors must be considered, including fragmentation and slot granularity.

**Syntax**

``` yaml
  storage_pool_types:
  - kind: <storage pool name>
    pool_config:
      box_id: 1
      encryption: <optional, set to 1 for data encryption at rest>
      erasure_species: <fault tolerance mode name - none, block-4-2, or mirror-3-dc>
      kind: <storage pool name - assign the same value as above>
      pdisk_filter:
      - property:
        - type: <device type to match host_configs.drive.type>
      vdisk_kind: Default
  - kind: <storage pool name>
  ...
```


Each database in a cluster is assigned to at least one of available storage pools, chosen in a database creation operation. Pool kind names among those assigned can be referred to in a `DATA` parameter when defining column families in a [`CREATE TABLE`](../../yql/reference/syntax/create_table.md#column-family)/[`ALTER TABLE`](../../yql/reference/syntax/alter_table.md#column-family) YQL statement.

### State Storage configuration {#domains-state}

State Storage is an independent in-memory storage for volatile data which is being used in YDB internal processes. It keeps full data replicas across a set of assigned nodes. 

State Storage usually does not require scaling for performance, so number of nodes should be as small as possible yet fault tolerant.

State Storage availability is crucial for a YDB cluster, as it affects all databases regardless of the underlying Blob Storage pools. To ensure State Storage fault tolerance, the nodes must be selected in a way which guarantees a majority of working nodes in case of any expected failure. You may use the following guidelines to choose nodes for State Storage:

|Cluster type|Min number<br>of nodes|Choice guidelines|
|---------|-----------------------|-----------------|
|Fault-intolerant|1|Choose any single node of the cluster's Blob Storage.|
|Single availability zone|5|Choose five nodes in different fail domains of a block-4-2 Blob Storage pool to guarantee that 3 working nodes making up the majority (of 5) remain on any of two domains failure.|
|Geodistributed|9|Choose three nodes in different fail domains inside every of three availability zones of a mirror-3-dc Blob Storage pool to guarantee that 5 working nodes making up the majority (of 9) remain on failure of any availability zone + a fail domain.|

When deploying State Storage on a cluster which employs number of storage pools with possible mix of fault tolerance modes, consider increasing number of nodes to scatter across various storage pools, as unavailability of State Storage leads to unavailability of the whole cluster.

**Syntax**
``` yaml
state_storage:
- ring:
    node: <array of State Storage nodes>
    nto_select: <number of data replicas in State Storage>
  ssid: 1
```

Each State Storage client (for instance, a DataShard tablet) use `nto_select` nodes to replicate its data in State Storage. If State Storage contains more nodes than `nto_select`, different nodes can be used for different clients, so make sure that any subset of `nto_select` nodes within State Storage satisfy the fault tolerance criteria.

Use odd values for `nto_select`, as using even ones won't add to fault tolerance comparing to the nearest lesser odd value.

### Examples {#domains-examples}

{% list tabs %}

- Block-4-2

  ``` yaml
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: block-4-2
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8]
        nto_select: 5
      ssid: 1

- Mirror-3-dc

  ``` yaml
  domains_config:
    domain:
    - name: global
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: 1
          erasure_species: mirror-3-dc
          kind: ssd
          pdisk_filter:
          - property:
            - type: SSD
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 2, 3, 4, 5, 6, 7, 8, 9]
        nto_select: 9
      ssid: 1
  ```

- None

  ``` yaml
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
    state_storage:
    - ring:
        node:
        - 1
        nto_select: 1
      ssid: 1
  ```

- Multiple pools

  ``` yaml
  domains_config:
    domain:
    - name: Root
      storage_pool_types:
      - kind: ssd
        pool_config:
          box_id: '1'
          erasure_species: block-4-2
          kind: ssd
          pdisk_filter:
          - property:
            - {type: SSD}
          vdisk_kind: Default
      - kind: rot
        pool_config:
          box_id: '1'
          erasure_species: block-4-2
          kind: rot
          pdisk_filter:
          - property:
            - {type: ROT}
          vdisk_kind: Default
      - kind: rotencrypted
        pool_config:
          box_id: '1'
          encryption_mode: 1
          erasure_species: block-4-2
          kind: rotencrypted
          pdisk_filter:
          - property:
            - {type: ROT}
          vdisk_kind: Default
      - kind: ssdencrypted
        pool_config:
          box_id: '1'
          encryption_mode: 1
          erasure_species: block-4-2
          kind: ssdencrypted
          pdisk_filter:
          - property:
            - {type: SSD}
          vdisk_kind: Default
    state_storage:
    - ring:
        node: [1, 16, 31, 46, 61, 76, 91, 106]
        nto_select: 5
      ssid: 1
  ```

{% endlist %}

## actor_system_config - Actor system {#actor-system}

Create an actor system configuration. Specify how processor cores will be distributed across the pools of cores available in the system.

```bash
actor_system_config:
  executor:
  - name: System
    spin_threshold: 0
    threads: 2
    type: BASIC
  - name: User
    spin_threshold: 0
    threads: 3
    type: BASIC
  - name: Batch
    spin_threshold: 0
    threads: 2
    type: BASIC
  - name: IO
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: IO
  - name: IC
    spin_threshold: 10
    threads: 1
    time_per_mailbox_micro_secs: 100
    type: BASIC
  scheduler:
    progress_threshold: 10000
    resolution: 256
    spin_threshold: 0
```

{% note warning %}

Make sure the total number of cores assigned to the IC, Batch, System, and User pools does not exceed the number of available system cores.

{% endnote %}

## blob_storage_config - Сluster static group {#blob-storage-config}

Specify a static cluster group's configuration. A static group is necessary for the operation of the basic cluster tablets, including `Hive`, `SchemeShard`, and `BlobstorageContoller`.
As a rule, these tablets do not store a lot of data, so we don't recommend creating more than one static group.

For a static group, specify the disks and nodes that the static group will be placed on. For example, a configuration for the `erasure: none` model can be as follows:

```bash
blob_storage_config:
  service_set:
    groups:
    - erasure_species: none
      rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: 1
            path: /dev/disk/by-partlabel/ydb_disk_ssd_02
            pdisk_category: SSD
....
```

For a configuration located in 3 availability zones, specify 3 rings. For a configuration within a single availability zone, specify exactly one ring.

## Sample cluster configurations {#examples}

The [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/) provides model examples of cluster configurations for self-deployment. Check them out before deploying a cluster.

