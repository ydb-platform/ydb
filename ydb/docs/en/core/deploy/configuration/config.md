# Creating a configuration for deploying a cluster

To deploy a {{ ydb-short-name }} cluster, you need to create a cluster configuration.
This section describes how to create a {{ ydb-short-name }} cluster configuration in YAML format.

## Description of storage host configurations

Create and describe a host configuration. For each host configuration, specify the sequence number and a list of paths to disks and their types.
The following disk types are available: `SSD`, `NVME`, and `ROT` (in this case, `ROT` disks are `HDD`).

For example:

```bash
host_configs:
- drive:
  - path: /dev/disk/by-partlabel/ydb_disk_ssd_01
    type: SSD
  host_config_id: 1
```

In this example, we can find exactly one type of host whose sequence number is 1. In this host configuration, exactly one disk is specified, its type is `SSD` and path is `/dev/disk/by-partlabel/ydb_disk_ssd_01`.

Below is another configuration example. Let's assume that we have two types of host configurations, one of them with 2 disks available on the host and the other one with 3 disks.
This configuration can be specified as follows:

```bash
host_configs:
- drive:
  - path: /dev/disk/by-partlabel/ydb_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_ssd_02
    type: SSD
  host_config_id: 1
- drive:
  - path: /dev/disk/by-partlabel/ydb_ssd_01
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_ssd_02
    type: SSD
  - path: /dev/disk/by-partlabel/ydb_ssd_03
    type: SSD
  host_config_id: 2
```

## Description of cluster hosts

List the hosts to run a cluster on. For each host, specify the sequence number and the port where `Interconnect` will be run on this host.
You also need to specify the physical location of the host and the unique ID of the host configuration.

For example,

```bash
hosts:
- host: hostname1
  host_config_id: 1
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

## Description of a cluster domain configuration

Describe a cluster domain configuration. Specify the domain name, storage types, the numbers of the hosts that will be included in `StateStorage`, and the `nto_select` parameter.
In the storage configuration, specify the type of storage and the type of storage fault tolerance (`erasure`), which will be used to initialize the database storage.
You should also specify what types of disks this storage type will correspond to. The following models of storage fault tolerance are available:

* The `block-4-2` configuration assumes deployment in 8 fail domains (by default, fail domains are racks) and can withstand a failure of no more than 2 fail domains.
* The `mirror-3-dc` configuration assumes deployment in 3 data centers, each of which has 3 fault tolerance domains and can withstand a failure of one data center and one more fault tolerance domain (rack).
* The `none` configuration doesn't provide fault tolerance but is convenient for functional testing.

The `StateStorage` fault tolerance is defined by the `nto_select` parameter. The `StateStorage` configuration is fault-tolerant if any subset of `nto_select` servers that are part of `StateStorage` is fault-tolerant.
`StateStorage` remains available if most hosts are available for any subset of `nto_select` servers that are part of `StateStorage`.

For example, if a cluster uses the `block-4-2` fault tolerance model for its storage, to make `StateStorage` fault-tolerant, set the `nto_select` parameter to 5 and place the hosts in 5 different availability domains.
If the cluster uses the `mirror-3-dc` fault tolerance model, to make `StateStorage` fault-tolerant, set the `nto_select` parameter to 9 and place the hosts in 3 data centers with 3 availability domains in each of them.

{% note warning %}

Make sure the NToSelect value is odd. For the proper operation of `StateStorage`, make sure that most of the NToSelect replicas are available for an arbitrary set of NToSelect hosts in `StateStorage`.

{% endnote %}

For example, you can use the following configuration for functional testing on a single server:

```bash
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

In this case, a domain is named `Root` and storage of the `SSD` type is created in it. This type of storage corresponds to disks with the `type` parameter set to `SSD`.
The `erasure_species: none` line in the parameter indicates that storage is created without fault tolerance.

If a cluster is located in three availability zones with 3 servers available in each of them, it may have the following configuration:

```bash
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

In this case, a domain is named `global` and storage of the `SSD` type is also created in it. The `erasure_species: mirror-3-dc` line indicates that storage is created with the `mirror-3-dc` fault tolerance model. `StateStorage` will include 9 servers with the `nto_select` parameter set to 9.

## Description of an actor system configuration

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

## Description of a static cluster group

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
            path: /dev/disk/by-partlabel/ydb_ssd_02
            pdisk_category: SSD
....
```

For a configuration located in 3 availability zones, specify 3 rings. For a configuration within a single availability zone, specify exactly one ring.

## Sample cluster configurations

The [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/) provides model examples of cluster configurations for self-deployment. Check them out before deploying a cluster.

