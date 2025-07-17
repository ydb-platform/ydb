# {{ ydb-short-name }} cluster configuration

The cluster configuration is specified in the YAML file passed in the `--yaml-config` parameter when the cluster nodes are run.

This article describes the main groups of configurable parameters in this file.

## host_configs: Typical host configurations {#host-configs}

A {{ ydb-short-name }} cluster consists of multiple nodes, and one or more typical server configurations are usually used for their deployment. To avoid repeating its description for each node, there is a `host_configs` section in the configuration file that lists the used configurations and assigned IDs.

### Syntax

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

### Examples

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

The {{ ydb-short-name }} Kubernetes operator mounts NBS disks for Storage nodes at the path `/dev/kikimr_ssd_00`. To use them, the following `host_configs` configuration must be specified:

```yaml
host_configs:
- host_config_id: 1
  drive:
  - path: /dev/kikimr_ssd_00
    type: SSD
```

The example configuration files provided with the {{ ydb-short-name }} Kubernetes operator contain this section, and it does not need to be changed.

## hosts: Static cluster nodes {#hosts}

This group lists the static cluster nodes on which the Storage processes run and specifies their main characteristics:

- Numeric node ID
- DNS host name and port that can be used to connect to a node on the IP network
- ID of the [standard host configuration](#host-configs)
- Placement in a specific availability zone, rack
- Server inventory number (optional)

### Syntax

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

### Examples

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

When deploying {{ ydb-short-name }} with a Kubernetes operator, the entire `hosts` section is generated automatically, replacing any user-specified content in the configuration passed to the operator. All Storage nodes use `host_config_id` = `1`, for which the [correct configuration](#host-configs-k8s) must be specified.

## domains_config: Cluster domain {#domains-config}

This section contains the configuration of the {{ ydb-short-name }} cluster root domain, including the [Blob Storage](#domains-blob) (binary object storage), [State Storage](#domains-state), and [authentication](#auth) configurations.

### Syntax

```yaml
domains_config:
  domain:
  - name: <root domain name>
    storage_pool_types: <Blob Storage configuration>
  state_storage: <State Storage configuration>
  security_config: <authentication configuration>
```

## Blob Storage configuration {#domains-blob}

This section defines one or more types of storage pools available in the cluster for the data in the databases with the following configuration options:

- Storage pool name
- Device properties (for example, disk type)
- Data encryption (on/off)
- Fault tolerance mode

The following [fault tolerance modes](../../concepts/topology.md) are available:

| Mode | Description |
| --- | --- |
| `none` | There is no redundancy. Applies for testing. |
| `block-4-2` | Redundancy factor of 1.5, applies to single data center clusters. |
| `mirror-3-dc` | Redundancy factor of 3, applies to multi-data center clusters. |

### Syntax

```yaml
  storage_pool_types:
  - kind: <storage pool name>
    pool_config:
      box_id: 1
      encryption_mode: <optional, specify 1 to encrypt data on the disk>
      erasure_species: <fault tolerance mode name - none, block-4-2, or mirror-3-dc>
      kind: <storage pool name - specify the same value as above>
      pdisk_filter:
      - property:
        - type: <device type to be compared with the one specified in host_configs.drive.type>
      vdisk_kind: Default
  - kind: <storage pool name>
  ...
```

Each database in the cluster is assigned at least one of the available storage pools selected in the database creation operation. The names of storage pools among those assigned can be used in the `DATA` attribute when defining column groups in YQL operators [`CREATE TABLE`](../../yql/reference/syntax/create_table/family.md)/[`ALTER TABLE`](../../yql/reference/syntax/alter_table/family.md).

## State Storage configuration {#domains-state}

State Storage is an independent in-memory storage for variable data that supports internal {{ ydb-short-name }} processes. It stores data replicas on multiple assigned nodes.

State Storage usually does not need scaling for better performance, so the number of nodes in it must be kept as small as possible taking into account the required level of fault tolerance.

State Storage availability is key for a {{ ydb-short-name }} cluster because it affects all databases, regardless of which storage pools they use. To ensure fault tolerance of State Storage, its nodes must be selected to guarantee a working majority in case of expected failures.

The following guidelines can be used to select State Storage nodes:

| Cluster type | Min number of<br/>nodes | Selection guidelines |
|---------|-----------------------|-----------------|
| Without fault tolerance | 1 | Select one random node. |
| Within a single availability zone | 5 | Select five nodes in different block-4-2 storage pool failure domains to ensure that a majority of 3 working nodes (out of 5) remain when two domains fail. |
| Geo-distributed | 9 | Select three nodes in different failure domains within each of the three mirror-3-dc storage pool availability zones to ensure that a majority of 5 working nodes (out of 9) remain when the availability zone + failure domain fail. |

When deploying State Storage on clusters that use multiple storage pools with a possible combination of fault tolerance modes, consider increasing the number of nodes and spreading them across different storage pools because unavailability of State Storage results in unavailability of the entire cluster.

### Syntax

```yaml
state_storage:
- ring:
    node: <StateStorage node array>
    nto_select: <number of data replicas in StateStorage>
  ssid: 1
```

Each State Storage client (for example, DataShard tablet) uses `nto_select` nodes to write copies of its data to State Storage. If State Storage consists of more than `nto_select` nodes, different nodes can be used for different clients, so you must ensure that any subset of `nto_select` nodes within State Storage meets the fault tolerance criteria.

Odd numbers must be used for `nto_select` because using even numbers does not improve fault tolerance in comparison to the nearest smaller odd number.

## Authentication configuration {#auth}

The [authentication mode](../../security/authentication.md) in the {{ ydb-short-name }} cluster is created in the `domains_config.security_config` section.

### Syntax

```yaml
domains_config:
  ...
  security_config:
    # authentication mode settings
    enforce_user_token_requirement: false
    enforce_user_token_check_requirement: false
    default_user_sids: <SID list for anonymous requests>
    all_authenticated_users: <group SID for all authenticated users>
    all_users_group: <group SID for all users>

    # initial security settings
    default_users: <initial list of users>
    default_groups: <initial list of groups>
    default_access: <initial permissions>

    # access level settings
    viewer_allowed_sids: <list of SIDs enabled for YDB UI access>
    monitoring_allowed_sids: <list of SIDs enabled for tablet administration>
    administration_allowed_sids: <list of SIDs enabled for storage administration>
    register_dynamic_node_allowed_sids: <list of SIDs enabled for database node registration>
  ...
```

| Key | Description |
| --- | --- |
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
   ```


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
   ```


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
| --- | --- |
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
| --- | --- |
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

There are many components inside {{ ydb-short-name }} [database nodes](../../concepts/glossary.md#database-node) that utilize memory. Most of them need a fixed amount, but some are flexible and can use varying amounts of memory, typically to improve performance. If {{ ydb-short-name }} components allocate more memory than is physically available, the operating system is likely to [terminate](https://en.wikipedia.org/wiki/Out_of_memory#Recovery) the entire {{ ydb-short-name }} process, which is undesirable. The memory controller's goal is to allow {{ ydb-short-name }} to avoid out-of-memory situations while still efficiently using the available memory.

Examples of components managed by the memory controller:

- [Shared cache](../../concepts/glossary.md#shared-cache): stores recently accessed data pages read from [distributed storage](../../concepts/glossary.md#distributed-storage) to reduce disk I/O and accelerate data retrieval.
- [MemTable](../../concepts/glossary.md#memtable): holds data that has not yet been flushed to [SST](../../concepts/glossary.md#sst).
- [KQP](../../concepts/glossary.md#kqp): stores intermediate query results.
- Allocator caches: keep memory blocks that have been released but not yet returned to the operating system.

Memory limits can be configured to control overall memory usage, ensuring the database operates efficiently within the available resources.

### Hard memory limit {#hard-memory-limit}

The hard memory limit specifies the total amount of memory available to {{ ydb-short-name }} process.

By default, the hard memory limit for {{ ydb-short-name }} process is set to its [cgroups](https://en.wikipedia.org/wiki/Cgroups) memory limit.

In environments without a cgroups memory limit, the default hard memory limit equals to the host's total available memory. This configuration allows the database to utilize all available resources but may lead to resource competition with other processes on the same host. Although the memory controller attempts to account for this external consumption, such a setup is not recommended.

Additionally, the hard memory limit can be specified in the configuration. Note that the database process may still exceed this limit. Therefore, it is highly recommended to use cgroups memory limits in production environments to enforce strict memory control.

Most of other memory limits can be configured either in absolute bytes or as a percentage relative to the hard memory limit. Using percentages is advantageous for managing clusters with nodes of varying capacities. If both absolute byte and percentage limits are specified, the memory controller uses a combination of both (maximum for lower limits and minimum for upper limits).

Example of the `memory_controller_config` section with a specified hard memory limit:

```yaml
memory_controller_config:
  hard_limit_bytes: 16106127360
```

### Soft memory limit {#soft-memory-limit}

The soft memory limit specifies a dangerous threshold that should not be exceeded by {{ ydb-short-name }} process under normal circumstances.

If the soft limit is exceeded, {{ ydb-short-name }} gradually reduces the [shared cache](../../concepts/glossary.md#shared-cache) size to zero. Therefore, more database nodes should be added to the cluster as soon as possible, or per-component memory limits should be reduced.

### Target memory utilization {#target-memory-utilization}

The target memory utilization specifies a threshold for {{ ydb-short-name }} process memory usage that is considered optimal.

Flexible cache sizes are calculated according to their limit thresholds to keep process consumption around this value.

For example, in a database that consumes a little memory on query execution, caches consume memory around this threshold, and other memory stays free. If query execution consumes more memory, caches start to reduce their sizes to their minimum threshold.

### Per-component memory limits

There are two different types of components within {{ ydb-short-name }}.

The first type, known as cache components, functions as caches, for example, by storing the most recently used data. Each cache component has minimum and maximum memory limit thresholds, allowing them to adjust their capacity dynamically based on the current {{ ydb-short-name }} process consumption.

The second type, known as activity components, allocates memory for specific activities, such as query execution or the [compaction](../../concepts/glossary.md#compaction) process. Each activity component has a fixed memory limit. Additionally, there is a total memory limit for these activities from which they attempt to draw the required memory.

Many other auxiliary components and processes operate alongside the {{ ydb-short-name }} process, consuming memory. Currently, these components do not have any memory limits.

#### Cache components memory limits

The cache components include:

- Shared cache
- MemTable

Each cache component's limits are dynamically recalculated every second to ensure that each component consumes memory proportionally to its limit thresholds while the total consumed memory stays close to the target memory utilization.

The minimum memory limit threshold for cache components isn't reserved, meaning the memory remains available until it is actually used. However, once this memory is filled, the components typically retain the data, operating within their current memory limit. Consequently, the sum of the minimum memory limits for cache components is expected to be less than the target memory utilization.

If needed, both the minimum and maximum thresholds should be overridden; otherwise, any missing threshold will have a default value.

Example of the `memory_controller_config` section with specified shared cache limits:

```yaml
memory_controller_config:
  shared_cache_min_percent: 10
  shared_cache_max_percent: 30
```

#### Activity components memory limits

The activity components include:

- KQP

The memory limit for each activity component specifies the maximum amount of memory it can attempt to use. However, to prevent the {{ ydb-short-name }} process from exceeding the soft memory limit, the total consumption of activity components is further constrained by an additional limit known as the activities memory limit. If the total memory usage of the activity components exceeds this limit, any additional memory requests will be denied.

As a result, while the combined individual limits of the activity components might collectively exceed the activities memory limit, each component's individual limit should be less than this overall cap. Additionally, the sum of the minimum memory limits for the cache components, plus the activities memory limit, must be less than the soft memory limit.

There are some other activity components that currently do not have individual memory limits.

Example of the `memory_controller_config` section with a specified KQP limit:

```yaml
memory_controller_config:
  query_execution_limit_percent: 25
```

### Configuration parameters

Each configuration parameter applies within the context of a single database node.

As mentioned above, the sum of the minimum memory limits for the cache components plus the activities memory limit should be less than the soft memory limit.

This restriction can be expressed in a simplified form:

$shared\_cache\_min\_percent + mem\_table\_min\_percent + activities\_limit\_percent < soft\_limit\_percent$

Or in a detailed form:

$Max(shared\_cache\_min\_percent * hard\_limit\_bytes / 100, shared\_cache\_min\_bytes) + Max(mem\_table\_min\_percent * hard\_limit\_bytes / 100, mem\_table\_min\_bytes) + Min(activities\_limit\_percent * hard\_limit\_bytes / 100, activities\_limit\_bytes) < Min(soft\_limit\_percent * hard\_limit\_bytes / 100, soft\_limit\_bytes)$

| Parameter | Default | Description |
| --- | --- | --- |
| `hard_limit_bytes` | CGroup&nbsp;memory&nbsp;limit&nbsp;/<br/>Host memory | Hard memory usage limit. |
| `soft_limit_percent`&nbsp;/<br/>`soft_limit_bytes` | 75% | Soft memory usage limit. |
| `target_utilization_percent`&nbsp;/<br/>`target_utilization_bytes` | 50% | Target memory utilization. |
| `activities_limit_percent`&nbsp;/<br/>`activities_limit_bytes` | 30% | Activities memory limit. |
| `shared_cache_min_percent`&nbsp;/<br/>`shared_cache_min_bytes` | 20% | Minimum threshold for the shared cache memory limit. |
| `shared_cache_max_percent`&nbsp;/<br/>`shared_cache_max_bytes` | 50% | Maximum threshold for the shared cache memory limit. |
| `mem_table_min_percent`&nbsp;/<br/>`mem_table_min_bytes` | 1% | Minimum threshold for the MemTable memory limit. |
| `mem_table_max_percent`&nbsp;/<br/>`mem_table_max_bytes` | 3% | Maximum threshold for the MemTable memory limit. |
| `query_execution_limit_percent`&nbsp;/<br/>`query_execution_limit_bytes` | 20% | KQP memory limit. |

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

## Configuring authentication providers {#auth-config}

{{ ydb-short-name }} supports various user authentication methods. The configuration for authentication providers is specified in the `auth_config` section.

### A Password Complexity Policies {#password-complexity}

{{ ydb-short-name }} allows users to be authenticated by login and password. More details can be found in the section [authentication by login and password](../../security/authentication.md#static-credentials). To enhance security in {{ ydb-short-name }} it is possible to configure the complexity of user passwords. You can enable the password complexity policy due include addition section `password_complexity`.

Syntax of the `password_complexity` section:

```yaml
auth_config:
  #...
  password_complexity:
    min_length: 8
    min_lower_case_count: 1
    min_upper_case_count: 1
    min_numbers_count: 1
    min_special_chars_count: 1
    special_chars: "!@#$%^&*()_+{}|<>?="
    can_contain_username: false
  #...
```

| Parameter | Description | Default value
|:---|:---|:---:|
| `min_length` | Minimal length of the password | 0 |
| `min_lower_case_count` | Minimal count of letters in lower case | 0 |
| `min_upper_case_count` | Minimal cont of letters in upper case | 0 |
| `min_numbers_count` | Minimal count of number in the password | 0 |
| `min_special_chars_count` | Minimal count of special chars in the password from list `special_chars`| 0 |
| `special_chars` | Special characters which can be used in the password. Allow use chars from list `!@#$%^&*()_+{}\|<>?=` only. Value (`""`) is equivalent to list `!@#$%^&*()_+{}\|<>?=` | Empty list. Equivalent to all allowed characters: `!@#$%^&*()_+{}\|<>?=` |
| `can_contain_username` | Allow use username in the password | `false` |

{% note info %}

Any changes to the password policy do not affect existing user passwords, so it is not necessary to change current passwords; they will be accepted as they are.

{% endnote %}

### Account lockout after unsuccessful password attempts {#account-lockout}

{{ ydb-short-name }} allows for the blocking of user authentication after unsuccessful password entry attempts. Lockout rules are configured in the `account_lockout` section.

Syntax of the `account_lockout` section:

```yaml
auth_config:
  #...
  account_lockout:
    attempt_threshold: 4
    attempt_reset_duration: "1h"
  #...
```

| Parameter | Description | Default value |
| :--- | :--- | :---: |
| `attempt_threshold` | The maximum number of unsuccessful password entry attempts. After `attempt_threshold` unsuccessful attempts, the user will be locked out for the duration specified in the `attempt_reset_duration` parameter. A zero value for the `attempt_threshold` parameter indicates no restrictions on the number of password entry attempts. After successful authentication (correct username and password), the counter for unsuccessful attempts is reset to 0. | 4 |
| `attempt_reset_duration` | The duration of the user lockout period. During this period, the user will not be able to authenticate in the system even if the correct username and password are entered. The lockout period starts from the moment of the last incorrect password attempt. If a zero ("0s" - a notation equivalent to 0 seconds) lockout period is set, the user will be considered locked out indefinitely. In this case, the system administrator must lift the lockout.<br/><br/>The minimum lockout duration is 1 second.<br/>Supported time units:<ul><li>Seconds: `30s`</li><li>Minutes: `20m`</li><li>Hours: `5h`</li><li>Days: `3d`</li></ul>It is not allowed to combine time units in one entry. For example, the entry "1d12h" is incorrect. It should be replaced with an equivalent, such as "36h". | "1h" |

### Configuring LDAP authentication {#ldap-auth-config}

One of the user authentication methods in {{ ydb-short-name }} is with an LDAP directory. More details about this type of authentication can be found in the section on [interacting with the LDAP directory](../../security/authentication.md#ldap-auth-provider). To configure LDAP authentication, the `ldap_authentication` section must be defined.

Example of the `ldap_authentication` section:

```yaml
auth_config:
  #...
  ldap_authentication:
    hosts:
      - "ldap-hostname-01.example.net"
      - "ldap-hostname-02.example.net"
      - "ldap-hostname-03.example.net"
    port: 389
    base_dn: "dc=mycompany,dc=net"
    bind_dn: "cn=serviceAccaunt,dc=mycompany,dc=net"
    bind_password: "serviceAccauntPassword"
    search_filter: "uid=$username"
    use_tls:
      enable: true
      ca_cert_file: "/path/to/ca.pem"
      cert_require: DEMAND
  ldap_authentication_domain: "ldap"
  scheme: "ldap"
  requested_group_attribute: "memberOf"
  extended_settings:
      enable_nested_groups_search: true

  refresh_time: "1h"
  #...
```

| Parameter | Description |
| --- | --- |
| `hosts` | A list of hostnames where the LDAP server is running. |
| `port` | The port used to connect to the LDAP server. |
| `base_dn` | The root of the subtree in the LDAP directory from which the user entry search begins. |
| `bind_dn` | The Distinguished Name (DN) of the service account used to search for the user entry. |
| `bind_password` | The password for the service account used to search for the user entry. |
| `search_filter` | A filter for searching the user entry in the LDAP directory. The filter string can include the sequence *$username*, which is replaced with the username requested for authentication in the database. |
| `use_tls` | Configuration settings for the TLS connection between {{ ydb-short-name }} and the LDAP server. |
| `enable` | Determines if a TLS connection [using the `StartTls` request](../../security/authentication.md#starttls) will be attempted. When set to `true`, the `ldaps` connection scheme should be disabled by setting `ldap_authentication.scheme` to `ldap`. |
| `ca_cert_file` | The path to the certification authority's certificate file. |
| `cert_require` | Specifies the certificate requirement level for the LDAP server.<br>Possible values:<ul><li>`NEVER` - {{ ydb-short-name }} does not request a certificate or accepts any presented certificate.</li><li>`ALLOW` - {{ ydb-short-name }} requests a certificate from the LDAP server but will establish the TLS session even if the certificate is not trusted.</li><li>`TRY` - {{ ydb-short-name }} requires a certificate from the LDAP server and terminates the connection if it is not trusted.</li><li>`DEMAND`/`HARD` - These are equivalent to `TRY` and are the default setting, with the value set to `DEMAND`.</li></ul> |
| `ldap_authentication_domain` | An identifier appended to the username to distinguish LDAP directory users from those authenticated using other providers. The default value is `ldap`. |
| `scheme` | The connection scheme to the LDAP server.<br>Possible values:<ul><li>`ldap` - Connects without encryption, sending passwords in plain text. This is the default value.</li><li>`ldaps` - Connects using TLS encryption from the first request. To use `ldaps`, disable the [`StartTls` request](../../security/authentication.md#starttls) by setting `ldap_authentication.use_tls.enable` to `false`, and provide certificate details in `ldap_authentication.use_tls.ca_cert_file` and set the certificate requirement level in `ldap_authentication.use_tls.cert_require`.</li><li>Any other value defaults to `ldap`.</li></ul> |
| `requested_group_attribute` | The attribute used for reverse group membership. The default is `memberOf`. |
| `extended_settings.enable_nested_groups_search` | A flag indicating whether to perform a request to retrieve the full hierarchy of groups to which the user's direct groups belong. |
| `host` | The hostname of the LDAP server. This parameter is deprecated and should be replaced with the `hosts` parameter. |
| `refresh_time` | Specifies the interval for refreshing user information. The actual update will occur within the range from `refresh_time/2` to `refresh_time`. |

## Enabling stable node names {#node-broker-config}

Node names are assigned through the Node Broker, which is a system tablet that registers dynamic nodes in the {{ ydb-short-name }} cluster.

Node Broker assigns names to dynamic nodes when they register in the cluster. By default, a node name consists of the hostname and the port on which the node is running.

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


## `feature_flags` configuration section {#feature_flags}

To enable a {{ ydb-short-name }} feature, set the corresponding feature flag in the `feature_flags` section of the cluster configuration. For example, to enable support for vector indexes and auto-partitioning of topics in the CDC, you need to add the following lines to the configuration:

```yaml
feature_flags:
  enable_vector_index: true
  enable_topic_autopartitioning_for_cdc: true
```

### Feature flags

| Flag          | Feature |
|---------------------------| ----------------------------------------------------|
| `enable_vector_index`                                    | Support for [vector indexes](../../dev/vector-indexes.md) for approximate vector similarity search |
| `enable_batch_updates`                                   | Support for `BATCH UPDATE` and `BATCH DELETE` statements |
| `enable_kafka_native_balancing`                          | Client balancing of partitions when reading using the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) |
| `enable_topic_autopartitioning_for_cdc`                  | [Auto-partitioning topics](../../concepts/cdc.md#topic-partitions) for row-oriented tables in CDC |
| `enable_access_to_index_impl_tables`                     | Support for [followers (read replicas)](../../yql/reference/syntax/alter_table/indexes.md) for covered secondary indexes |
| `enable_changefeeds_export`, `enable_changefeeds_import` | Support for changefeeds in backup and restore operations |
| `enable_view_export`                                     | Support for views in backup and restore operations |
| `enable_export_auto_dropping`                            | Automatic cleanup of temporary tables and directories during export to S3 |
| `enable_followers_stats`                                 | System views with information about [history of overloaded partitions](../../dev/system-views.md#top-overload-partitions) |
| `enable_strict_acl_check`                                | Strict ACL checks — do not allow granting rights to non-existent users and delete users with permissions |
| `enable_strict_user_management`                          | Strict checks for local users — only the cluster or database administrator can administer local users |
| `enable_database_admin`                                  | The role of a database administrator |

## Configuring Health Check {#healthcheck-config}

This section configures thresholds and timeout settings used by the {{ ydb-short-name }} [health check service](../ydb-sdk/health-check-api.md). These parameters help configure detection of potential [issues](../ydb-sdk/health-check-api.md#issues), such as excessive restarts or time drift between dynamic nodes.

### Syntax

```yaml
healthcheck_config:
  thresholds:
    node_restarts_yellow: 10
    node_restarts_orange: 30
    nodes_time_difference_yellow: 5000
    nodes_time_difference_orange: 25000
    tablets_restarts_orange: 30
  timeout: 20000
```

### Parameters

| Parameter                                 | Default | Description                                                                   |
|-------------------------------------------|---------|-------------------------------------------------------------------------------|
| `thresholds.node_restarts_yellow`         | `10`    | Number of node restarts to trigger a `YELLOW` warning                         |
| `thresholds.node_restarts_orange`         | `30`    | Number of node restarts to trigger an `ORANGE` alert                          |
| `thresholds.nodes_time_difference_yellow` | `5000`  | Max allowed time difference (in us) between dynamic nodes for `YELLOW` issue  |
| `thresholds.nodes_time_difference_orange` | `25000` | Max allowed time difference (in us) between dynamic nodes for `ORANGE` issue  |
| `thresholds.tablets_restarts_orange`      | `30`    | Number of tablet restarts to trigger an `ORANGE` alert                        |
| `timeout`                                 | `20000` | Maximum health check response time (in ms)                                    |

## Sample cluster configurations {#examples}

You can find model cluster configurations for deployment in the [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Check them out before deploying a cluster.
