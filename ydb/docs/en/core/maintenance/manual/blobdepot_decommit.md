# Group decommissioning

Physical groups are a valuable resource in a cluster: groups can be created, but they cannot be deleted without deleting the database that uses them, since there is no mechanism for guaranteed eviction of tablet data from a group. At the same time, the number of physical groups is determined by the cluster size, and groups cannot be moved from one tenant's pool to another tenant's pool due to the use of different encryption keys for different tenants.

This can lead to a situation where there are not enough resources to create a new group to expand an existing database or create a new one, and it is also impossible to delete an old group to free up resources, since it may contain data.

To solve this problem, you can create a virtual group with channels on top of the remaining groups in the pool, copy data from the physical group into it, and then free up the resources occupied by the physical group. This task is handled by the group decommissioning process.

Group decommissioning allows you to remove redundant VDisks from PDisks while preserving the data of that group. This mode is implemented by creating a blob storage that starts serving the decommissioned group instead of the DS proxy. In parallel, the blob storage copies data from the physical decommissioned group. Once all data has been copied, the physical VDisks are deleted and resources are freed, with all data of the decommissioned group distributed across other groups.

The decommissioning process is completely transparent to tablets and the user and consists of several stages:

1. Creating a blob storage tablet and distributing the group configuration to block writes to the disks of the physical group.
2. Copying lock metadata from the physical group. After this point, the decommissioned group becomes available for operation. Until the locks are copied, working with the group is impossible. However, this process takes a very short time, so it is practically unnoticeable to the client. Requests arriving at this moment are queued and wait for the stage to complete.
3. Copying barrier metadata from the physical group.
4. Copying blob metadata from the physical group.
5. Copying blob data from the physical group.
6. Deleting the VDisks of the physical group.

It is worth noting once again that from the moment writes to the physical group are blocked until all locks are read, work with the group is suspended. Under normal operation, the suspension time is a fraction of a second.

## How to run

To start decommissioning, run the BS\_CONTROLLER command, in which you need to specify the list of decommissioned groups, as well as the number of the Hive tablet that will manage the blob storages of the decommissioned groups. You can also specify a list of pools in which the blob storage will store its data. If this list is not specified, BS\_CONTROLLER automatically selects for data storage the same pools in which the decommissioned groups are located, and the number of data channels is made equal to the number of physical groups in these pools (but no more than 250).

```bash
dstool -e ... --direct group decommit --group-ids 2181038080 --database=/Root/db1 --wait
```

Command line parameters:

* --wait wait for the decommissioning to start; if a startup error occurs, the error is displayed on the screen and the decommissioning is automatically canceled (only when this option is specified);
* --group-ids GROUP\_ID GROUP\_ID list of groups for which decommissioning can be performed;
* --database=DB specify the tenant in which to perform decommissioning (or the domain, if decommissioning is performed for groups within the domain);
* --log-channel-sp=POOL\_NAME name of the pool in which channel 0 of the blob storage tablet will be placed;
* --snapshot-channel-sp=POOL\_NAME name of the pool in which channel 1 of the blob storage tablet will be placed; if not specified, the value from --log-channel-sp is used;
* --data-channel-sp=POOL\_NAME[\*COUNT] name of the pool in which data channels are placed; if the COUNT parameter is specified (after the "asterisk" sign), then COUNT data channels are created in the specified pool.

If neither --log-channel-sp, nor --snapshot-channel-sp, nor --data-channel-sp are specified, then the storage pool to which the decommissioned group belongs is automatically found, and in it the zero and first channels of the blob storage are created, as well as N data channels, where N is the number of remaining physical groups in this pool.

## How to check that everything has started {#decommit-check-running}

You can view the result of decommissioning similarly to creating virtual groups. For decommissioned groups, an additional DecommitStatus field appears, which can take one of the following values:

* NONE — decommissioning is not performed for the specified group;
* PENDING — decommissioning of the group is expected, but not yet performed (a blob storage is being created);
* IN\_PROGRESS — decommissioning of the group is being performed (all writes already go to the blob storage, reads go to the blob storage and to the old group);
* DONE — decommissioning is fully completed.

```bash
$ dstool --cluster=$CLUSTER --direct group list --virtual-groups-only
┌────────────┬──────────────┬───────────────┬────────────┬────────────────┬─────────────────┬──────────────┬───────────────────┬──────────────────┬───────────────────┬─────────────┬────────────────┐
│ GroupId    │ BoxId:PoolId │ PoolName      │ Generation │ ErasureSpecies │ OperatingStatus │ VDisks_TOTAL │ VirtualGroupState │ VirtualGroupName │ BlobDepotId       │ ErrorReason │ DecommitStatus │
├────────────┼──────────────┼───────────────┼────────────┼────────────────┼─────────────────┼──────────────┼───────────────────┼──────────────────┼───────────────────┼─────────────┼────────────────┤
│ 2181038080 │ [1:1]        │ /Root:ssd     │ 2          │ block-4-2      │ FULL            │ 8            │ WORKING           │                  │ 72075186224038160 │             │ IN_PROGRESS    │
│ 2181038081 │ [1:1]        │ /Root:ssd     │ 2          │ block-4-2      │ FULL            │ 8            │ WORKING           │                  │ 72075186224038161 │             │ IN_PROGRESS    │
│ 4261412864 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg1              │ 72075186224037888 │             │ NONE           │
│ 4261412865 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg2              │ 72075186224037890 │             │ NONE           │
│ 4261412866 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg3              │ 72075186224037889 │             │ NONE           │
│ 4261412867 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg4              │ 72075186224037891 │             │ NONE           │
└────────────┴──────────────┴───────────────┴────────────┴────────────────┴─────────────────┴──────────────┴───────────────────┴──────────────────┴───────────────────┴─────────────┴────────────────┘
```

## How to estimate progress {#decommit-progress}

To estimate the time and progress of decommissioning, graphs are provided that allow you to understand:

* whether decommissioning is in progress (Decommit/GetBytes);
* whether data is being written (Decommit/PutOkBytes);
* how much data remains to be decommissioned (BytesToDecommit).

If everything executes successfully, the Decommit/GetBytes speed roughly corresponds to Decommit/PutOkBytes. Insignificant discrepancies are acceptable because the data being decommissioned may become outdated and be deleted by the tablet that stores the data in it.

To estimate the remaining decommission time, it is sufficient to divide BytesToDecommit by the average Decommit/PutOkBytes speed.
