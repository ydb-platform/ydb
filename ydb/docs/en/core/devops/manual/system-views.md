# Cluster system views

To enable internal introspection of the cluster state, the user can make queries to special system views. These tables are accessible from the cluster's root directory and use the `.sys` system path prefix.

Cloud database users usually don't have access to cluster system tables, as cluster support and timely diagnostics are the prerogatives of the cloud team.

Hereinafter, in the descriptions of available fields, the **Key** column contains the corresponding table's primary key field index.

{% note info %}

There are also similar system views describing what's happening inside a given database, they are covered in a [separate article for DBAs](../../dev/system-views.md).

{% endnote %}

## Distributed Storage

Information about the operation of distributed storage is contained in several interconnected tables, each of which is responsible for describing its entity, such as:

* PDisk
* VSlot
* Group
* Storage Pool

In addition, there is a separate table that shows statistics on the use of the number of groups in different storage pools and whether these pools can be increased.

### ds_pdisks

| **Field** | **Type** | **Key** | **Value** |
| ----------------------- | ----------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| NodeId | Uint32 | 0 | ID of the node where a PDisk is running. |
| PDiskId | Uint32 | 1 | ID of the PDisk (unique within the node). |
| Type | String |  | Media type (ROT, SSD, NVME). |
| Kind | Uint64 |  | A user-defined numeric ID that is needed to group disks with the same type of media into different subgroups. |
| Path | String |  | Path to the block device inside the machine. |
| Guid | Uint64 |  | A unique ID that is generated randomly when adding a disk to the system and is designed to prevent data loss in the event of disk swapping. |
| BoxId | Uint64 |  | ID of the Box that this PDisk belongs to. |
| SharedWithOs | Bool |  | Flag indicating if the "SharedWithOs" label is available. Set manually when creating a PDisk. You can use it to filter disks when creating new groups. |
| ReadCentric | Bool |  | Flag indicating if the "ReadCentric" label is available. Set manually when creating a PDisk. You can use it to filter disks when creating new groups. |
| AvailableSize | Uint64 |  | The number of bytes that can be allocated on the PDisk. |
| TotalSize | Uint64 |  | The total number of bytes on the PDisk. |
| Status | String |  | PDisk operation mode that affects its participation in the allocation of groups (ACTIVE, INACTIVE, BROKEN, FAULT, and TO_BE_REMOVED). |
| StatusChangeTimestamp | Timestamp |  | The time when the Status was last changed. NULL indicates that the Status hasn't changed since the creation of PDisk. |
| ExpectedSlotCount | Uint32 |  | The maximum number of VSlots that can be created on this PDisk. |
| NumActiveSlots | Uint32 |  | The number of slots that are currently active. |

### ds_vslots

| **Field** | **Type** | **Key** | **Value** |
| ----------------- | --------- | ---------- | ----------------------------------------------------------------------------------------- |
| NodeId | Uint32 | 0 | ID of the node where a VSlot is running. |
| PDiskId | Uint32 | 1 | ID of the PDisk inside the node where the VSlot is running. |
| VSlotId | Uint32 | 2 | ID of the VSlot inside the PDisk. |
| GroupId | Uint32 |  | Number of the storage group that this VSlot belongs to. |
| GroupGeneration | Uint32 |  | Generation of the storage group configuration that this VSlot belongs to. |
| FailRealm | Uint32 |  | Relative number of the fail realm of the VSlot within the storage group. |
| FailDomain | Uint32 |  | Relative number of the fail domain of the VSlot within the fail realm. |
| VDisk | Uint32 |  | Relative number of the VSlot inside the fail domain. |
| AllocatedSize | Uint64 |  | The number of bytes that the VSlot occupies on the PDisk. |
| AvailableSize | Uint64 |  | The number of bytes that can be allocated to this VSlot. |
| Status | String |  | Status of the VDisk running in this VSlot (INIT_PENDING, REPLICATING, READY, or ERROR). |
| Kind | String |  | Preset VDisk operation mode (Default, Log, ...). |

Please not that the (NodeId, PDiskId) tuple creates an external key to the `ds_pdisks` table and the (GroupId) to the `ds_groups` table.

### ds_groups

| **Field** | **Type** | **Key** | **Value** |
| --------------------- | ---------- | ---------- | ------------------------------------------------------------------------------------------------------------ |
| GroupId | Uint32 | 0 | Number of the storage group in the cluster. |
| Generation | Uint32 |  | Storage group configuration generation. |
| ErasureSpecies | String |  | Group redundancy coding mode (block-4-2, mirror-3-dc, mirror-3of4, ...). |
| BoxId | Uint64 |  | ID of the Box that this group is created in. |
| StoragePoolId | Uint64 |  | ID of the storage pool inside the Box that this group operates in. |
| EncryptionMode | Uint32 |  | Group data encryption and its algorithm (if enabled). |
| LifeCyclePhase | Uint32 |  | Availability of a generated encryption key (if encryption is enabled). |
| AllocatedSize | Uint64 |  | The number of allocated bytes of data in the group (reduced to user bytes, that is, to redundancy). |
| AvailableSize | Uint64 |  | The number of bytes of user data available for allocation (up to redundancy as well). |
| SeenOperational | Bool |  | A Boolean flag that indicates whether the group was operational after its creation. |
| PutTabletLogLatency | Interval |  | 90th percentile of the PutTabletLog request execution time. |
| PutUserDataLatency | Interval |  | 90th percentile of the PutUserData request execution time. |
| GetFastLatency | Interval |  | 90th percentile of the GetFast request execution time. |

In this table, the (BoxId, StoragePoolId) tuple creates an external key to the `ds_storage_pools` table.

### ds_storage_pools

| **Field** | **Type** | **Key** | **Value** |
| ---------------- | --------- | ---------- | ------------------------------------------------------------------------------------------------------------- |
| BoxId | Uint64 | 0 | ID of the Box that this storage pool belongs to. |
| StoragePoolId | Uint64 | 1 | ID of the storage pool inside the Box. |
| Name | String |  | User-defined storage pool name (used when linking tablets and storage pools). |
| Generation | Uint64 |  | Storage pool configuration generation (number of changes). |
| ErasureSpecies | String |  | Redundancy coding mode for all groups within this storage pool. |
| VDiskKind | String |  | Preset operation mode for all VDisks in this storage pool. |
| Kind | String |  | A user-defined string description of the purpose of the pool, which can also be used for filtering. |
| NumGroups | Uint32 |  | Number of groups within this storage pool. |
| EncryptionMode | Uint32 |  | Data encryption setting for all groups (similar to ds_groups.EncryptionMode). |
| SchemeshardId | Uint64 |  | ID of the SchemeShard object of the schema that this storage pool belongs to (as of now, always NULL). |
| PathId | Uint64 |  | ID of the node of the schema object inside the specified SchemeShard that this storage pool belongs to. |

### ds_storage_stats

Unlike other tables that show physical entities, the `ds_storage_stats` table shows aggregated storage statistics.

| **Field** | **Type** | **Key** | **Value** |
| ------------------------- | --------- | ---------- | ------------------------------------------------------------------------------------------------- |
| BoxId | Uint64 | 0 | ID of the Box that statistics are calculated for. |
| PDiskFilter | String | 1 | A string description of filters that select a PDisk to create groups (for example, by media type). |
| ErasureSpecies | String | 2 | Redundancy coding mode that statistics are collected for. |
| CurrentGroupsCreated | Uint32 |  | Number of groups created with the specified characteristics. |
| CurrentAllocatedSize | Uint64 |  | Total space occupied by all groups from CurrentGroupsCreated. |
| CurrentAvailableSize | Uint64 |  | Total space that is available to all groups from CurrentGroupsCreated. |
| AvailableGroupsToCreate | Uint32 |  | Number of groups with the specified characteristics that can be created taking into account the need for a reserve. |
| AvailableSizeToCreate | Uint64 |  | Number of available bytes that will be obtained when creating all groups from AvailableGroupsToCreate. |

It should be noted that AvailableGroupsToCreate shows the maximum number of groups that can be created if no other types of groups are created. So when extending a storage pool, the count of AvailableGroupsToCreate in several rows of statistics may change.


{% note info %}

Loads caused by accessing system views are more analytical in nature. Making frequent queries to them in large DBs will consume a lot of system resources. The recommended load is no more than 1-2 RPS.

{% endnote %}
