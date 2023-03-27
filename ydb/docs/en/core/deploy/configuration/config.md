# Cluster configuration

The cluster configuration is specified in the YAML file passed in the `--yaml-config` parameter when the cluster nodes are run.

This article describes the main groups of configurable parameters in this file.

## host_configs: Typical host configurations {#host-configs}

A YDB cluster consists of multiple nodes, and one or more typical server configurations are usually used for their deployment. To avoid repeating its description for each node, there is a `host_configs` section in the configuration file that lists the used configurations and the assigned IDs.

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

The `host_config_id` attribute specifies a numeric configuration ID. The `drive` attribute contains a collection of descriptions of connected drives. Each description consists of two attributes:

- `path`: Path to the mounted block device, for example, `/dev/disk/by-partlabel/ydb_disk_ssd_01`
- `type`: Type of the device's physical media: `ssd`, `nvme`, or `rot` (rotational - HDD)

**Examples**

One configuration with ID 1, one SSD disk accessible via `/dev/disk/by-partlabel/ydb_disk_ssd_01`:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
```

Two configurations with IDs 1 (two SSD disks) and 2 (three SSD disks):

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

### Kubernetes features {#host-configs-k8s}

YDB Kubernetes operator mounts NBS disks for Storage nodes on the path `/dev/kikimr_ssd_00`. To use them, the following `host_configs` configuration must be specified:

``` yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

The configuration example files supplied as part of YDB Kubernetes operator contain such a section, and it does not need to be changed.

## hosts: Static cluster nodes {#hosts}

This group lists the static cluster nodes on which the Storage processes run and specifies their main characteristics:

- Numeric node ID
- DNS host name and port that can be used to connect to a node on the IP network
- ID of the [typical host configuration](#host-configs)
- Placement in a specific availability zone, rack
- Server serial number (optional)

**Syntax**

``` yaml
hosts:
- host: <DNS host name>
  host_config_id: <numeric ID of the typical host configuration>
  port: <port> # 19001 by default
  location:
    unit: <string with the server serial number>
    data_center: <string with the availability zone ID>
    rack: <string with the rack ID>
- host: <DNS host name>
  ...
```

**Examples**

``` yaml
hosts:
- host: hostname1
  host_config_id: 1
  node_id: 1
  port: 19001
  location:
    unit: '1'
    data_center: '1'
    rack: '1'
- host: hostname2
  host_config_id: 1
  node_id: 2
  port: 19001
  location:
    unit: '1'
    data_center: '1'
    rack: '1'
```

### Kubernetes features {#hosts-k8s}

When deploying YDB with a Kubernetes operator, the entire `hosts` section is generated automatically, replacing any user-specified content in the configuration passed to the operator. All Storage nodes use `host_config_id` = `1`, for which the [correct configuration](#host-configs-k8s) must be specified.

## domains_config: Cluster domain {#domains-config}

This section contains the configuration of the YDB cluster root domain, including the [Blob Storage](#domains-blob) (binary object storage), [State Storage](#domains-state), and [authentication](#auth) configurations.

**Syntax**

``` yaml
domains_config:
  domain:
  - name: <root domain name>
    storage_pool_types: <Blob Storage configuration>
  state_storage: <State Storage configuration>
  security_config: <authentication configuration>
```

### Blob Storage configuration {#domains-blob}

This section defines one or more types of storage pools available in the cluster for the data in the databases with the following configuration options:

- Storage pool name
- Device properties (for example, disk type)
- Data encryption (on/off)
- Fault tolerance mode

The following [fault tolerance modes](../../cluster/topology.md) are available:

| Mode | Description |
--- | ---
| `none` | There is no redundancy. Applied for testing. |
| `block-4-2` | Redundancy with a factor of 1.5, applies to single data center clusters. |
| `mirror-3-dc` | Redundancy with a factor of 3, applies to multi data center clusters. |
| `mirror-3dc-3-nodes` | Redundancy with a factor of 3. Applied for testing. |

**Syntax**

``` yaml
  storage_pool_types:
  - kind: <storage pool name>
    pool_config:
      box_id: 1
      encryption: <optional, specify 1 to encrypt data on the disc>
      erasure_species: <fault tolerance mode name - none, block-4-2, or mirror-3-dc>
      kind: <storage pool name - specify the same value as above>
      pdisk_filter:
      - property:
        - type: <device type to be compared with the one specified in host_configs.drive.type>
      vdisk_kind: Default
  - kind: <storage pool name>
  ...
```

Each database in the cluster is assigned at least one of the available storage pools selected in the database creation operation. The names of storage pools among those assigned can be used in the `DATA` attribute when defining column groups in YQL operators [`CREATE TABLE`](../../yql/reference/syntax/create_table.md#column-family)/[`ALTER TABLE`](../../yql/reference/syntax/alter_table.md#column-family).

### State Storage configuration {#domains-state}

State Storage is an independent in-memory storage for modifiable data that supports internal YDB processes. It stores data replicas on multiple assigned nodes.

State Storage usually does not need scaling for better performance, so the number of nodes in it must be kept as small as possible taking into account the required level of fault tolerance.

State Storage availability is key for a YDB cluster because it affects all databases, regardless of which storage pools they use. To ensure fault tolerance of State Storage, its nodes must be selected in such a way as to guarantee a working majority in case of expected failures.

The following guidelines can be used to select State Storage nodes:

| Cluster type | Min number of<br>nodes | Selection guidelines |
|---------|-----------------------|-----------------|
| Without fault tolerance | 1 | Select one random node. |
| Within a single availability zone | 5 | Select five nodes in different block-4-2 storage pool failure domains to ensure that a majority of 3 working nodes (out of 5) remain when two domains fail. |
| Geo-distributed | 9 | Select three nodes in different failure domains within each of the three mirror-3-dc storage pool availability zones to ensure that a majority of 5 working nodes (out of 9) remain when the availability zone + failure domain fail. |

When deploying State Storage on clusters that use multiple storage pools with a possible combination of fault tolerance modes, consider increasing the number of nodes and spreading them across different storage pools because unavailability of State Storage results in unavailability of the entire cluster.

**Syntax**
``` yaml
state_storage:
- ring:
    node: <StateStorage node array>
    nto_select: <number of data replicas in StateStorage>
  ssid: 1
```

Each State Storage client (for example, DataShard tablet) uses `nto_select` nodes to write copies of its data to State Storage. If State Storage consists of more than `nto_select` nodes, different nodes can be used for different clients, so you must ensure that any subset of `nto_select` nodes within State Storage meets the fault tolerance criteria.

Odd numbers must be used for `nto_select` because using even numbers does not improve fault tolerance in comparison with the nearest smaller odd number.

### Authentication configuration {#auth}

The [authentication mode](../../concepts/auth.md) in the {{ ydb-short-name }} cluster is created in the `domains_config.security_config` section.

**Syntax**

``` yaml
domains_config:
  ...
  security_config:
    enforce_user_token_requirement: Bool
  ...
```

Key | Description 
--- | ---
 `enforce_user_token_requirement` | Require a user token.</br>Acceptable values:</br><ul><li>`false`: Anonymous authentication mode, no token needed (default value, applied when the parameter is not specified).</li><li>`true`: Username/password authentication mode. A valid user token is needed for authentication.</li></ul> 

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

- Block-4-2 + Auth

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
    security_config:
      enforce_user_token_requirement: true

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

- No fault tolerance

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

## Actor system {#actor-system}

The `actor_system_config` section specifies the number of node CPU cores to be allocated to different {{ ydb-full-name }} cluster subsystems.

The CPU resources are mainly used by the actor system. Depending on the type, all actors run in one of the pools (the `name` parameter). Configuring is allocating a node's CPU cores across the actor system pools. When allocating them, please keep in mind that PDisks and the gRPC API run outside the actor system and require separate resources.

```yaml
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

| Parameter | Description |
--- | ---
| `executor` | Pool configuration.<br>You should only change the number of CPU cores (the `threads` parameter) in the pool configs. |
| `name` | Pool name that indicates its purpose. Possible values:<ul><li>`System`: A pool that is designed for running quick internal operations in {{ ydb-full-name }} (it serves system tablets, state storage, distributed storage I/O, and erasure coding).</li><li>`User`: A pool that serves the user load (user tablets, queries run in the Query Processor).</li><li>`Batch`: A pool that serves tasks with no strict limit on the execution time, background operations like garbage collection and heavy queries run in the Query Processor.</li><li>`IO`: A pool responsible for performing any tasks with blocking operations (such as authentication or writing logs to a file).</li><li>`IC`: Interconnect, it serves the load related to internode communication (system calls to wait for sending and send data across the network, data serialization, as well as message splits and merges).</li></ul> |
| `spin_threshold` | The number of CPU cycles before going to sleep if there are no messages. In sleep mode, there is less power consumption, but it may increase request latency under low loads. |
| `threads` | The number of CPU cores allocated per pool.<br>Make sure the total number of cores assigned to the System, User, Batch, and IC pools does not exceed the number of available system cores. |
| `time_per_mailbox_micro_secs` | The number of messages per actor to be handled before switching to a different actor. |
| `type` | Pool type. Possible values:<ul><li>`IO` should be set for IO pools.</li><li>`BASIC` should be set for any other pool.</li></ul> |
| `scheduler` | Scheduler configuration. The actor system scheduler is responsible for the delivery of deferred messages exchanged by actors.<br>We do not recommend changing the default scheduler parameters. |
| `progress_threshold` | The actor system supports requesting message sending scheduled for a later point in time. The system might fail to send all scheduled messages at some point. In this case, it starts sending them in "virtual time" by handling message sending in each loop over a period that doesn't exceed the `progress_threshold` value in microseconds and shifting the virtual time by the `progress_threshold` value until it reaches real time. |
| `resolution` | When making a schedule for sending messages, discrete time slots are used. The slot duration is set by the `resolution` parameter in microseconds. |

## blob_storage_config: Static cluster group {#blob-storage-config}

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

You can find model cluster configurations for deployment in the [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Check them out before deploying a cluster.
