# {{ ydb-short-name }} cluster configuration

The cluster configuration is specified in the YAML file passed in the `--yaml-config` parameter when the cluster nodes are run.

This article describes the main groups of configurable parameters in this file.

## host_configs: Typical host configurations {#host-configs}

A YDB cluster consists of multiple nodes, and one or more typical server configurations are usually used for their deployment. To avoid repeating its description for each node, there is a `host_configs` section in the configuration file that lists the used configurations and assigned IDs.

**Syntax**

```yaml
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

### Kubernetes features {#host-configs-k8s}

The YDB Kubernetes operator mounts NBS disks for Storage nodes at the path `/dev/kikimr_ssd_00`. To use them, the following `host_configs` configuration must be specified:

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

The example configuration files provided with the YDB Kubernetes operator contain this section, and it does not need to be changed.

## hosts: Static cluster nodes {#hosts}

This group lists the static cluster nodes on which the Storage processes run and specifies their main characteristics:

- Numeric node ID
- DNS host name and port that can be used to connect to a node on the IP network
- ID of the [standard host configuration](#host-configs)
- Placement in a specific availability zone, rack
- Server inventory number (optional)

**Syntax**

```yaml
hosts:
- host: <DNS host name>
  host_config_id: <numeric ID of the standard host configuration>
  port: <port> # 19001 by default
  location:
    unit: <string with the server serial number>
    data_center: <string with the availability zone ID>
    rack: <string with the rack ID>
- host: <DNS host name>
  ...
```

**Examples**

```yaml
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

```yaml
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

The following [fault tolerance modes](../../concepts/topology.md) are available:

| Mode | Description |
--- | ---
| `none` | There is no redundancy. Applies for testing. |
| `block-4-2` | Redundancy factor of 1.5, applies to single data center clusters. |
| `mirror-3-dc` | Redundancy factor of 3, applies to multi-data center clusters. |
| `mirror-3dc-3-nodes` | Redundancy factor of 3. Applies for testing. |

**Syntax**

```yaml
  storage_pool_types:
  - kind: <storage pool name>
    pool_config:
      box_id: 1
      encryption: <optional, specify 1 to encrypt data on the disk>
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

State Storage is an independent in-memory storage for variable data that supports internal YDB processes. It stores data replicas on multiple assigned nodes.

State Storage usually does not need scaling for better performance, so the number of nodes in it must be kept as small as possible taking into account the required level of fault tolerance.

State Storage availability is key for a YDB cluster because it affects all databases, regardless of which storage pools they use. To ensure fault tolerance of State Storage, its nodes must be selected to guarantee a working majority in case of expected failures.

The following guidelines can be used to select State Storage nodes:

| Cluster type | Min number of<br/>nodes | Selection guidelines |
|---------|-----------------------|-----------------|
| Without fault tolerance | 1 | Select one random node. |
| Within a single availability zone | 5 | Select five nodes in different block-4-2 storage pool failure domains to ensure that a majority of 3 working nodes (out of 5) remain when two domains fail. |
| Geo-distributed | 9 | Select three nodes in different failure domains within each of the three mirror-3-dc storage pool availability zones to ensure that a majority of 5 working nodes (out of 9) remain when the availability zone + failure domain fail. |

When deploying State Storage on clusters that use multiple storage pools with a possible combination of fault tolerance modes, consider increasing the number of nodes and spreading them across different storage pools because unavailability of State Storage results in unavailability of the entire cluster.

**Syntax**
```yaml
state_storage:
- ring:
    node: <StateStorage node array>
    nto_select: <number of data replicas in StateStorage>
  ssid: 1
```

Each State Storage client (for example, DataShard tablet) uses `nto_select` nodes to write copies of its data to State Storage. If State Storage consists of more than `nto_select` nodes, different nodes can be used for different clients, so you must ensure that any subset of `nto_select` nodes within State Storage meets the fault tolerance criteria.

Odd numbers must be used for `nto_select` because using even numbers does not improve fault tolerance in comparison to the nearest smaller odd number.

### Authentication configuration {#auth}

The [authentication mode](../../concepts/auth.md) in the {{ ydb-short-name }} cluster is created in the `domains_config.security_config` section.

**Syntax**

```yaml
domains_config:
  ...
  security_config:
    enforce_user_token_requirement: Bool
  ...
```

| Key | Description |
--- | ---
| `enforce_user_token_requirement` | Require a user token.<br/>Acceptable values:<br/><ul><li>`false`: Anonymous authentication mode, no token needed (used by default if the parameter is omitted).</li><li>`true`: Username/password authentication mode. A valid user token is needed for authentication.</li></ul> |

### Examples {#domains-examples}

{% list tabs %}

- `block-4-2`

   ```yaml
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


- `block-4-2` + Auth

   ```yaml
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


- `mirror-3-dc`

   ```yaml
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

- `none` (without fault tolerance)

   ```yaml
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

   ```yaml
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

The CPU resources are mainly used by the actor system. Depending on the type, all actors run in one of the pools (the `name` parameter). Configuring is allocating a node's CPU cores across the actor system pools. When allocating them, please keep in mind that PDisks and the gRPC API run outside the actor system and require separate resources.

You can set up your actor system either [automatically](#autoconfig) or [manually](#tuneconfig). In the `actor_system_config` section, specify:

* Node type and the number of CPU cores allocated to the ydbd process by automatic configuring.
* Number of CPU cores for each {{ ydb-full-name }} cluster subsystem in the case of manual configuring.

Automatic configuring adapts to the current system workload. It is recommended in most cases.

You might opt for manual configuring when a certain pool in your actor system is overwhelmed and undermines the overall database performance. You can track the workload on your pools on the [Embedded UI monitoring page](../../reference/embedded-ui/ydb-monitoring.md#node_list_page).

### Automatic configuring {#autoconfig}

Example of the `actor_system_config` section for automatic configuring of the actor system:

```yaml
actor_system_config:
  use_auto_config: true
  node_type: STORAGE
  cpu_count: 10
```

| Parameter | Description |
--- | ---
| `use_auto_config` | Enabling automatic configuring of the actor system. |
| `node_type` | Node type. Determines the expected workload and vCPU ratio between the pools. Possible values:<ul><li>`STORAGE`: The node interacts with network block store volumes and is responsible for managing the Distributed Storage.</li><li>`COMPUTE`: The node processes the workload generated by users.</li><li>`HYBRID`: The node is used for hybrid load or the usage of `System`, `User`, and `IC` for the node under load is about the same. |
| `cpu_count` | Number of vCPUs allocated to the node. |

### Manual configuring {#tuneconfig}

Example of the `actor_system_config` section for manual configuring of the actor system:

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
| `executor` | Pool configuration.<br/>You should only change the number of CPU cores (the `threads` parameter) in the pool configs. |
| `name` | Pool name that indicates its purpose. Possible values:<ul><li>`System`: A pool that is designed for running quick internal operations in {{ ydb-full-name }} (it serves system tablets, state storage, distributed storage I/O, and erasure coding).</li><li>`User`: A pool that serves the user load (user tablets, queries run in the Query Processor).</li><li>`Batch`: A pool that serves tasks with no strict limit on the execution time, background operations like garbage collection and heavy queries run in the Query Processor.</li><li>`IO`: A pool responsible for performing any tasks with blocking operations (such as authentication or writing logs to a file).</li><li>`IC`: Interconnect, it serves the load related to internode communication (system calls to wait for sending and send data across the network, data serialization, as well as message splits and merges).</li></ul> |
| `spin_threshold` | The number of CPU cycles before going to sleep if there are no messages. In sleep mode, there is less power consumption, but it may increase request latency under low loads. |
| `threads` | The number of CPU cores allocated per pool.<br/>Make sure the total number of cores assigned to the System, User, Batch, and IC pools does not exceed the number of available system cores. |
| `max_threads` | Maximum vCPU that can be allocated to the pool from idle cores of other pools. When you set this parameter, the system enables the mechanism of expanding the pool at full utilization, provided that idle vCPUs are available.<br/>The system checks the current utilization and reallocates vCPUs once per second.  |
| `max_avg_ping_deviation` | Additional condition to expand the pool's vCPU. When more than 90% of vCPUs allocated to the pool are utilized, you need to worsen SelfPing by more than `max_avg_ping_deviation` microseconds from 10 milliseconds expected. |
| `time_per_mailbox_micro_secs` | The number of messages per actor to be handled before switching to a different actor. |
| `type` | Pool type. Possible values:<ul><li>`IO` should be set for IO pools.</li><li>`BASIC` should be set for any other pool.</li></ul> |
| `scheduler` | Scheduler configuration. The actor system scheduler is responsible for the delivery of deferred messages exchanged by actors.<br/>We do not recommend changing the default scheduler parameters. |
| `progress_threshold` | The actor system supports requesting message sending scheduled for a later point in time. The system might fail to send all scheduled messages at some point. In this case, it starts sending them in "virtual time" by handling message sending in each loop over a period that doesn't exceed the `progress_threshold` value in microseconds and shifting the virtual time by the `progress_threshold` value until it reaches real time. |
| `resolution` | When making a schedule for sending messages, discrete time slots are used. The slot duration is set by the `resolution` parameter in microseconds. |

## Memory controller {#memory-controller}

There are many components inside {{ ydb-short-name }} [nodes](../../concepts/glossary.md) that utilize memory. Most of them need a fixed amount, but some are flexible and can use varying amounts of memory, typically to gain performance improvement. If such components allocate more memory than is physically available, the operating system is likely to [terminate](https://en.wikipedia.org/wiki/Out_of_memory#Recovery) the entire {{ ydb-short-name }} node, which is undesirable. The memory controller's goal is to allow {{ ydb-short-name }} to avoid out-of-memory situations while still efficiently using the available memory.

Examples of components managed by the memory controller:

- Shared Cache: Stores recently accessed data pages read from Blob Storage to reduce disk I/O and accelerate data retrieval;
- MemTable: Holds data that has not yet been flushed to SST;
- KQP: Executes queries and stores their intermediate results;
- Allocator Caches: Keeps memory blocks which have been released but not yet returned to the operating system.

Memory limits can be configured to control the overall memory usage, ensuring the database operates efficiently within the available resources.

### Hard memory limit {#hard-memory-limit}

The hard memory limit specifies the total amount of available memory.

By default, the hard memory limit is set to the {{ ydb-short-name }} node process's [cgroups](https://en.wikipedia.org/wiki/Cgroups) memory limit. 

In environments without a cgroups memory limit, the default hard memory limit is equal to the host's total available memory. This allows the database to utilize all available resources in unrestricted environments, though it may lead to resource contention with other processes running on the same host.

Additionally, the hard memory limit can be specified in the configuration. Note that the database process may still exceed this limit, so it is highly recommended to use cgroups memory limits in production environments to enforce strict memory control.

Example of the `memory_controller_config` section with a specified hard memory limit:

```yaml
memory_controller_config:
  hard_limit_bytes: 16106127360
```

### Per component memory limits

Certain {{ ydb-short-name }} components support limiting their individual memory usage. Most memory limits have both minimum and maximum thresholds, allowing for dynamic adjustment based on current process consumption.

Memory limits can be configured either in absolute bytes or as a percentage relative to the [hard memory limit](#hard-memory-limit). Using percentages is advantageous for managing clusters with nodes of varying capacities. If both absolute byte and percentage limits are specified, the memory controller uses a combination of both.

Example of the `memory_controller_config` section with specified Shared Cache limits:

```yaml
memory_controller_config:
  shared_cache_min_percent: 10
  shared_cache_max_percent: 30
```

Parameters | Description | Default
--- | --- | ---
`hard_limit_bytes` | A hard memory usage limit for the database. | CGroup&nbsp;memory&nbsp;limit&nbsp;/<br/>Host memory
`soft_limit_percent`&nbsp;/<br/>`soft_limit_bytes` | A soft memory usage limit for the database. When this threshold is exceeded, the database starts to reduce the Shared Cache size to zero. | 75%
`target_utilization_percent`&nbsp;/<br/>`target_utilization_bytes` | An ideal target for memory usage. Optimal cache sizes are calculated to keep process consumption around this value. | 50%
`shared_cache_min_percent`&nbsp;/<br/>`shared_cache_min_bytes` | A minimum threshold for the Shared Cache memory limit. | 20%
`shared_cache_max_percent`&nbsp;/<br/>`shared_cache_max_bytes` | A maximum threshold for the Shared Cache memory limit. | 50%
`mem_table_min_percent`&nbsp;/<br/>`mem_table_min_bytes` | A minimum threshold for the MemTable memory limit. | 1%
`mem_table_max_percent`&nbsp;/<br/>`mem_table_max_bytes` | A maximum threshold for the MemTable memory limit. | 3%

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

## BlobStorage production configurations

To ensure the required fault tolerance of {{ ydb-short-name }}, configure the [cluster disk subsystem](../../concepts/cluster/distributed_storage.md) properly: select the appropriate [fault tolerance mode](#fault-tolerance) and [hardware configuration](#requirements) for your cluster.

### Fault tolerance modes {#fault-tolerance}

We recommend using the following [fault tolerance modes](../../concepts/topology.md) for {{ ydb-short-name }} production installations:

* `block-4-2`: For a cluster hosted in a single availability zone.
* `mirror-3-dc`: For a cluster hosted in three availability zones.

A fail model of {{ ydb-short-name }} is based on concepts such as fail domain and fail realm.

Fail domain

: A set of hardware that may fail concurrently.

  For example, a fail domain includes disks of the same server (as all server disks may be unavailable if the server PSU or network controller is down). A fail domain also includes servers located in the same server rack (as the entire hardware in the rack may be unavailable if there is a power outage or some issue with the network hardware in the same rack).

  Any domain fail is handled automatically, without shutting down the system.

Fail realm

: A set of fail domains that may fail concurrently.

  An example of a fail realm is hardware located in the same data center that may fail as a result of a natural disaster.

Usually a fail domain is a server rack, while a fail realm is a data center.

When creating a [storage group](../../concepts/glossary.md#storage-groups), {{ ydb-short-name }} groups VDisks that are located on PDisks from different fail domains. For `block-4-2` mode, a PDisk should be distributed across at least 8 fail domains, and for `mirror-3-dc` mode, across 3 fail realms with at least 3 fail domains in each of them.

### Hardware configuration {#requirements}

If a disk fails, {{ ydb-short-name }} may automatically reconfigure a storage group so that, instead of the VDisk located on the failed hardware, a new VDisk is used that the system tries to place on hardware that is running while the group is being reconfigured. In this case, the same rule applies as when creating a group: a VDisk is created in a fail domain that is different from the fail domains of any other VDisk in this group (and in the same fail realm as that of the failed VDisk for `mirror-3-dc`).

This causes some issues when a cluster's hardware is distributed across the minimum required amount of fail domains:

* If the entire fail domain is down, reconfiguration no longer makes sense, since a new VDisk can only be located in the fail domain that is down.
* If part of a fail domain is down, reconfiguration is possible, but the load that was previously handled by the failed hardware will only be redistributed across hardware in the same fail domain.

If the number of fail domains in a cluster exceeds the minimum amount required for creating storage groups at least by one (that is, 9 domains for `block-4-2` and 4 domains in each fail realm for `mirror-3-dc)`, in case some hardware fails, the load can be redistributed across all the hardware that is still running.

The system can work with fail domains of any size. However, if there are few domains and a different number of disks in different domains, the number of storage groups that you can create will be limited. In this case, some hardware in fail domains that are too large may be underutilized. If the hardware is used in full, significant distortions in domain sizes may make reconfiguration impossible.

For example, there are 15 racks in a cluster with `block-4-2` fault tolerance mode. The first of the 15 racks hosts 20 servers and the other 14 racks host 10 servers each. To fully utilize all the 20 servers from the first rack, {{ ydb-short-name }} will create groups so that 1 disk from this largest fail domain is used in each group. As a result, if any other fail domain's hardware is down, the load can't be distributed to the hardware in the first rack.

{{ ydb-short-name }} can group disks of different vendors, capacity, and speed. The resulting characteristics of a group depend on a set of the worst characteristics of the hardware that is serving the group. Usually the best results can be achieved if you use same-type hardware. When creating large clusters, keep in mind that hardware from the same batch is more likely to have the same defect and fail simultaneously.

Therefore, we recommend the following optimal hardware configurations for production installations:

* **A cluster hosted in 1 availability zone**: It uses `block4-2` fault tolerance mode and consists of 9 or more racks with the same amount of identical servers in each rack.
* **A cluster hosted in 3 availability zones**: It uses `mirror3-dc` fault tolerance mode and is distributed across 3 data centers with 4 or more racks in each of them, the racks being equipped with the same amount of identical servers.

See also [{#T}](#reduced).

### Redundancy recovery {#rebuild}

Auto reconfiguration of storage groups reduces the risk of data loss in the event of multiple failures that occur within intervals sufficient to recover the redundancy. By default, reconfiguration is done one hour after {{ ydb-short-name }} detects a failure.

Once a group is reconfigured, a new VDisk is automatically populated with data to restore the required storage redundancy in the group. This increases the load on other VDisks in the group and the network. To reduce the impact of redundancy recovery on the system performance, the total data replication speed is limited both on the source and target VDisks.

The time it takes to restore the redundancy depends on the amount of data and hardware performance. For example, replication on fast NVMe SSDs may take an hour, while on large HDDs more than 24 hours. To make reconfiguration possible in general, a cluster should have free slots for creating VDisks in different fail domains. When determining the number of slots to be kept free, factor in the risk of hardware failure, the time it takes to replicate data and replace the failed hardware.

### Simplified hardware configurations {#reduced}

If it's not possible to use the [recommended amount](#requirements) of hardware, you can divide servers within a single rack into two dummy fail domains. In this configuration, a failure of 1 rack means a failure of 2 domains and not a single one. In [both fault tolerance modes](#fault-tolerance), {{ ydb-short-name }} will keep running if 2 domains fail. If you use the configuration with dummy fail domains, the minimum number of racks in a cluster is 5 for `block-4-2` mode and 2 in each data center for `mirror-3-dc` mode.

### Fault tolerance level {#reliability}

The table below describes fault tolerance levels for different fault tolerance modes and hardware configurations of a {{ ydb-short-name }} cluster:

Fault tolerance<br/>mode | Fail<br/>domain | Fail<br/>realm | Number of<br/>data centers | Number of<br/>server racks | Fault tolerance<br/>level
:--- | :---: | :---: | :---: | :---: | :---
`block-4-2` | Rack | Data center | 1 | 9 or more | Can stand a failure of 2 racks
`block-4-2` | ½ a rack | Data center | 1 | 5 or more | Can stand a failure of 1 rack
`block-4-2` | Server | Data center | 1 | Doesn't matter | Can stand a failure of 2 servers
`mirror-3-dc` | Rack | Data center | 3 | 4 in each data center | Can stand a failure of a data center and 1 rack in one of the two other data centers
`mirror-3-dc` | Server | Data center | 3 | Doesn't matter | Can stand a failure of a data center and 1 server in one of the two other data centers

## Node Broker Configuration {#node-broker-config}

Node Broker is a system tablet that registers dynamic nodes in the {{ ydb-short-name }} cluster.

Node broker gives names to dynamic nodes when they register. By default, a node name consists of the hostname and the port on which the node is running.

In a dynamic environment where hostnames often change, such as in Kubernetes, using hostname and port leads to an uncontrollable increase in the number of unique node names. This is true even for a database with a handful of dynamic nodes. Such behavior may be undesirable for a time series monitoring system as the number of metrics grows uncontrollably. To solve this problem, the system administrator can set up *stable* node names.

A stable name identifies a node within the tenant. It consists of a prefix and a node's sequential number within its tenant. If a dynamic node has been shut down, after a timeout, its stable name can be taken by a new dynamic node serving the same tenant.

To enable stable node names, you need to add the following to the cluster configuration:

```yaml
feature_flags:
  enable_stable_node_names: true
```

By default, the prefix is `slot-`. To override the prefix, add the following to the cluster configuration:

```yaml
node_broker_config:
  stable_node_name_prefix: <new prefix>
```

## Sample cluster configurations {#examples}

You can find model cluster configurations for deployment in the [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Check them out before deploying a cluster.
