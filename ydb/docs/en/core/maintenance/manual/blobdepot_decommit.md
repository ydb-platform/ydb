# Group Decommissioning

Physical groups are a valuable resource in the cluster: groups can be created, but they cannot be deleted without deleting the database that uses them, since there is no mechanism for guaranteed eviction of tablet data from the group. At the same time, the number of physical groups is determined by the cluster size, and groups cannot be moved from one tenant's pool to another tenant's pool due to the use of different encryption keys for different tenants.

This can lead to a situation where there are not enough resources to create a new group to expand an existing database or create a new database, and it is also impossible to delete an old group to free up resources, since it may contain data.

To solve this problem, you can create a virtual group with channels on top of the remaining groups in the pool, copy data from the physical group to it, and then free up the resources occupied by the physical group. This task is solved by the group decommissioning process.

Group decommissioning allows removing redundant VDisks from PDisks while preserving the data of this group. This mode is implemented by creating a blob depot that starts serving the decommissioned group instead of DS proxy. In parallel, the blob depot copies data from the physical decommissioned group. As soon as all data is copied, the physical VDisks are deleted and resources are freed, while all data from the decommissioned group is distributed across other groups.

The decommissioning process is completely transparent to tablets and users and consists of several stages:

1. Creating a blob depot tablet and distributing the group configuration to block writes to the physical group disks.
2. Copying lock metadata from the physical group. After this moment, the decommissioned group becomes available for work. Before the lock copying moment, working with the group is impossible. However, this process takes a very short time, so it is practically invisible to the client. Requests arriving at this moment are queued and wait for the stage to complete.
3. Copying barrier metadata from the physical group.
4. Copying blob metadata from the physical group.
5. Copying blob data from the physical group.
6. Deleting VDisks of the physical group.

It is worth noting once again that from the moment of blocking writes to the physical group until the moment of reading all locks, work with the group is suspended. The suspension time under normal operation is fractions of a second.

## How to Launch

To start decommissioning, a BS_CONTROLLER command is executed, in which you need to specify the list of groups to be decommissioned, as well as the number of the Hive tablet that will manage the blob depots of the decommissioned groups. You can also specify a list of pools where the blob depot will store its data. If this list is not specified, BS_CONTROLLER automatically selects the same pools where the decommissioned groups are located for data storage, and the number of data channels is made equal to the number of physical groups in these pools (but no more than 250).

```bash
dstool -e ... --direct group decommit --group-ids 2181038080 --database=/Root/db1 --hive-id=72057594037968897
```

Command line parameters:

* `--group-ids` GROUP_ID — GROUP_ID list of groups for which decommissioning can be performed.
* `--database=DB` — specify the tenant in which decommissioning should be done.
* `--hive-id=N` — explicitly specify the number of the Hive tablet that will manage this blob depot; you cannot specify the Hive identifier of the tenant that owns the pools with decommissioned groups, because this Hive may store its data on top of a group managed by the blob depot, which will lead to a circular dependency; it is recommended to specify the root Hive.
* `--log-channel-sp=POOL_NAME` — name of the pool where channel 0 of the blob depot tablet will be placed.
* `--snapshot-channel-sp=POOL_NAME` — name of the pool where channel 1 of the blob depot tablet will be placed; if not specified, the value from `--log-channel-sp` is used.
* `--data-channel-sp=POOL_NAME[*COUNT]` — name of the pool where data channels are placed; if the `COUNT` parameter is specified (after the asterisk), `COUNT` data channels are created in the specified pool.

If neither `--log-channel-sp`, nor `--snapshot-channel-sp`, nor `--data-channel-sp` are specified, then the storage pool to which the decommissioned group belongs is automatically found, and the zero and first channels of the blob depot are created in it, as well as N data channels, where N is the number of remaining physical groups in this pool.

## How to Check that Everything is Running {#decommit-check-running}

You can view the decommissioning result similarly to creating virtual groups. For decommissioned groups, an additional DecommitStatus field appears, which can take one of the following values:

* `NONE` — decommissioning is not performed for the specified group
* `PENDING` — group decommissioning is expected but not yet performed (blob depot is being created)
* `IN_PROGRESS` — group decommissioning is in progress (all writes already go to the blob depot, reads go to the blob depot and the old group)
* `DONE` — decommissioning is completely finished

```bash
$ dstool --cluster=$CLUSTER --direct group list --virtual-groups-only
┌────────────┬──────────────┬───────────────┬────────────┬────────────────┬─────────────────┬──────────────┬───────────────────┬──────────────────┬───────────────────┬─────────────┬────────────────┐
│ GroupId    │ BoxId:PoolId │ PoolName      │ Generation │ ErasureSpecies │ OperatingStatus │ VDisks_TOTAL │ VirtualGroupState │ VirtualGroupName │ BlobDepotId       │ ErrorReason │ DecommitStatus │
├────────────┼──────────────┼───────────────┼────────────┼────────────────┼─────────────────┼──────────────┼───────────────────┼──────────────────┼───────────────────┼─────────────┼────────────────┤
│ 2181038080 │ \[1:\]        │ /Root:ssd     │ 2          │ block-4-2      │ FULL            │ 8            │ WORKING           │                  │ 72075186224038160 │             │ IN_PROGRESS    │
│ 2181038081 │ \[1:1\]        │ /Root:ssd     │ 2          │ block-4-2      │ FULL            │ 8            │ WORKING           │                  │ 72075186224038161 │             │ IN_PROGRESS    │
│ 4261412864 │ \[1:2\]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg1              │ 72075186224037888 │             │ NONE           │
│ 4261412865 │ \[1:2\]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg2              │ 72075186224037890 │             │ NONE           │
│ 4261412866 │ \[1:2\]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg3              │ 72075186224037889 │             │ NONE           │
│ 4261412867 │ \[1:2\]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg4              │ 72075186224037891 │             │ NONE           │
└────────────┴──────────────┴───────────────┴────────────┴────────────────┴─────────────────┴──────────────┴───────────────────┴──────────────────┴───────────────────┴─────────────┴────────────────┘
```

## How to Assess Progress {#decommit-progress}

To assess the time and progress of decommissioning, charts are provided that allow you to understand:

* whether decommissioning is in progress (Decommit/GetBytes)
* whether data writing is happening (Decommit/PutOkBytes)
* how much data remains to be decommissioned (BytesToDecommit)

If everything is executed successfully, the Decommit/GetBytes rate approximately corresponds to Decommit/PutOkBytes. Minor discrepancies are acceptable due to the fact that decommissioned data may become outdated and be deleted by the tablet that stores data in it.

To estimate the remaining decommissioning time, it is sufficient to divide BytesToDecommit by the average Decommit/PutOkBytes rate.