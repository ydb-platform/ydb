# Cluster System Views

For internal introspection of cluster state, users can query special service views (system views). These views are available from the cluster root directory and use the system path prefix `.sys`.

Cloud database users typically do not have access to cluster system views, as the cloud team is responsible for their maintenance and timely diagnostics.

In the descriptions of available fields below, the **Key** column contains the primary key field index of the corresponding view.

{% note info %}

Similar system views exist for what happens inside a specific database, they are described in a [separate article for DBAs](../../dev/system-views.md).

{% endnote %}

## Distributed Storage

Information about distributed storage operation is contained in several interconnected views, each responsible for describing its own entity:

* PDisk
* VSlot
* Group
* Storage Pool

Additionally, there is a separate view that shows statistics on the usage of group numbers in different storage pools and the growth capabilities of these pools.

### ds_pdisks

| **Field**             | **Type**  | **Key** | **Value**                                                                                                                                                        |
|-----------------------|-----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NodeId                | Uint32    | 0       | Identifier of the node on which PDisk is running                                                                                                                 |
| PDiskId               | Uint32    | 1       | PDisk identifier (unique within the node)                                                                                                                        |
| Type                  | String    |         | Media type (ROT, SSD, NVME)                                                                                                                                      |
| Kind                  | Uint64    |         | User-defined numeric identifier needed to group disks with the same media type into different subgroups                                                          |
| Path                  | String    |         | Path to the block device inside the machine                                                                                                                       |
| Guid                  | Uint64    |         | Unique identifier randomly generated when adding a disk to the system, designed to prevent data loss in case disks are swapped                                   |
| BoxId                 | Uint64    |         | Identifier of the Box that includes this PDisk                                                                                                                   |
| SharedWithOs          | Bool      |         | Presence of the "SharedWithOs" label, set manually when creating PDisk. Can be used for filtering disks when creating new groups.                                |
| ReadCentric           | Bool      |         | Presence of the "ReadCentric" label, set manually when creating PDisk. Can be used for filtering disks when creating new groups.                                 |
| AvailableSize         | Uint64    |         | Number of bytes available for allocation on PDisk                                                                                                                |
| TotalSize             | Uint64    |         | Total number of bytes on PDisk                                                                                                                                   |
| Status                | String    |         | PDisk operating mode that affects its participation in group allocation (ACTIVE, INACTIVE, BROKEN, FAULTY, TO_BE_REMOVED)                                       |
| StatusChangeTimestamp | Timestamp |         | Time when Status last changed; if NULL, Status has not changed since PDisk creation                                                                              |
| ExpectedSlotCount     | Uint32    |         | Maximum number of slots (VSlot) that can be created on this PDisk. Either user-defined or inferred.                                                              |
| NumActiveSlots        | Uint32    |         | Number of currently occupied slots (VSlot) with respect to GroupSizeInUnits of VDisks                                                                            |
| SlotSizeInUnits       | Uint32    |         | Slot size in units. Either user-defined or inferrerd.                                                                                                            |
| DecommitStatus        | String    |         | Status of PDisk [decommissioning](../deployment-options/manual/decommissioning.md) (DECOMMIT_NONE, DECOMMIT_PENDING, DECOMMIT_IMMINENT, DECOMMIT_REJECTED)       |
| InferPDiskSlotCountFromUnitSize  | Uint64    |         | If not zero, ExpectedSlotCount and SlotSizeInUnits values are inferred (unless user-defined) from TotalSize and this value                           |

The inferred values are calculated by the formula `ExpectedSlotCount * SlotSizeInUnits = TotalSize / InferPDiskSlotCountFromUnitSize`, where `SlotSizeInUnits = 2^N` is chosen to meet `ExpectedSlotCount <= 16`.

### ds_vslots

| **Field**       | **Type** | **Key** | **Value**                                                                                       |
|-----------------|----------|---------|------------------------------------------------------------------------------------------------|
| NodeId          | Uint32   | 0       | Identifier of the node on which VSlot is running                                               |
| PDiskId         | Uint32   | 1       | PDisk identifier within the node on which VSlot is running                                     |
| VSlotId         | Uint32   | 2       | VSlot identifier within PDisk                                                                  |
| GroupId         | Uint32   |         | Storage group number that includes this VSlot                                                  |
| GroupGeneration | Uint32   |         | Storage group configuration generation that includes this VSlot                                |
| FailRealm       | Uint32   |         | Relative fail realm number of VSlot within the storage group                                   |
| FailDomain      | Uint32   |         | Relative fail domain number of VSlot within the fail realm                                     |
| VDisk           | Uint32   |         | Relative VSlot number within the fail domain                                                   |
| AllocatedSize   | Uint64   |         | Number of bytes that VSlot occupies on PDisk                                                   |
| AvailableSize   | Uint64   |         | Number of bytes available for allocation to this VSlot                                         |
| Status          | String   |         | State of the running VDisk in this VSlot (INIT_PENDING, REPLICATING, READY, ERROR)             |
| Kind            | String   |         | Preset VDisk operating mode setting (Default, Log, ...)                                        |

Note that the tuple (NodeId, PDiskId) forms a foreign key to the `ds_pdisks` view, and (GroupId) to the `ds_groups` view.

### ds_groups

| **Field**           | **Type** | **Key** | **Value**                                                                                                  |
|---------------------|----------|---------|-----------------------------------------------------------------------------------------------------------|
| GroupId             | Uint32   | 0       | Storage group number in the cluster                                                                       |
| Generation          | Uint32   |         | Storage group configuration generation                                                                     |
| ErasureSpecies      | String   |         | Redundancy encoding mode for the group (block-4-2, mirror-3-dc, mirror-3of4, ...)                        |
| BoxId               | Uint64   |         | Identifier of the Box in which this group was created                                                     |
| StoragePoolId       | Uint64   |         | Storage pool identifier within the Box where this group operates                                          |
| EncryptionMode      | Uint32   |         | Presence of data encryption in the group and encryption algorithm if enabled                              |
| LifeCyclePhase      | Uint32   |         | Presence of an expired encryption key if encryption is enabled                                            |
| AllocatedSize       | Uint64   |         | Amount of allocated data bytes in the group (converted to user bytes, i.e., before redundancy)           |
| AvailableSize       | Uint64   |         | Amount of user data bytes available for allocation (also before redundancy)                               |
| SeenOperational     | Bool     |         | Boolean flag showing whether the group was in operational state after its creation                        |
| PutTabletLogLatency | Interval |         | 90th percentile of PutTabletLog request execution time                                                    |
| PutUserDataLatency  | Interval |         | 90th percentile of PutUserData request execution time                                                     |
| GetFastLatency      | Interval |         | 90th percentile of GetFast request execution time                                                         |
| OperatingStatus     | String   |         | Group status based on latest VDisk reports only (UNKNOWN, FULL, PARTIAL, DEGRADED, DISINTEGRATED)         |
| ExpectedStatus      | String   |         | Status based not only on operational report, but on PDisk status and plans too (UNKNOWN, FULL, PARTIAL, DEGRADED, DISINTEGRATED) |
| GroupSizeInUnits    | Uint32   |         | Matching this value with PDisk.SlotSizeInUnits allows VDisks to acquire proportionally large storage size quota |

In this view, the tuple (BoxId, StoragePoolId) forms a foreign key to the `ds_storage_pools` view.

### ds_storage_pools

| **Field**      | **Type** | **Key** | **Value**                                                                                                   |
|----------------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| BoxId          | Uint64   | 0       | Identifier of the Box that includes this storage pool                                                       |
| StoragePoolId  | Uint64   | 1       | Storage pool identifier within the Box                                                                      |
| Name           | String   |         | User-defined storage pool name (used for linking tablets and storage pools)                                 |
| Generation     | Uint64   |         | Storage pool configuration generation (number of changes)                                                   |
| ErasureSpecies | String   |         | Redundancy encoding mode for all groups within this storage pool                                            |
| VDiskKind      | String   |         | Preset operating mode setting for all VDisks in this storage pool                                           |
| Kind           | String   |         | User-defined string description of pool purpose, can also be used for filtering                             |
| NumGroups      | Uint32   |         | Number of groups within this storage pool                                                                   |
| EncryptionMode | Uint32   |         | Data encryption setting for all groups (similar to ds_groups.EncryptionMode)                               |
| SchemeshardId  | Uint64   |         | SchemeShard identifier of the schema object to which this storage pool belongs (currently always NULL)      |
| PathId         | Uint64   |         | Schema object node identifier within the specified SchemeShard to which this storage pool belongs          |
| DefaultGroupSizeInUnits | Uint32   |         | The value of GroupSizeInUnits inherited by groups when new groups are added to the pool                                |

### ds_storage_stats

Unlike other views showing physical entities, `ds_storage_stats` shows aggregated storage information.

| **Field**               | **Type** | **Key** | **Value**                                                                                       |
|-------------------------|----------|---------|------------------------------------------------------------------------------------------------|
| BoxId                   | Uint64   | 0       | Box identifier for which statistics are calculated                                              |
| PDiskFilter             | String   | 1       | String description of filters selecting PDisk for group creation (e.g., by media type)         |
| ErasureSpecies          | String   | 2       | Redundancy encoding mode for which statistics are collected                                     |
| CurrentGroupsCreated    | Uint32   |         | Number of created groups with specified characteristics                                         |
| CurrentAllocatedSize    | Uint64   |         | Total occupied space across all groups included in CurrentGroupsCreated                        |
| CurrentAvailableSize    | Uint64   |         | Total space available for allocation across all groups included in CurrentGroupsCreated        |
| AvailableGroupsToCreate | Uint32   |         | Number of groups with specified characteristics that can be created considering reserve needs   |
| AvailableSizeToCreate   | Uint64   |         | Number of available bytes that would result from creating all groups from AvailableGroupsToCreate |

Note that AvailableGroupsToCreate shows the maximum number of groups that can be created if no other types of groups are created. Thus, when expanding one storage pool, the AvailableGroupsToCreate numbers in several statistics rows may change.

{% note info %}

Accessing system views is more of an analytical workload. Frequent access to them in large databases will significantly consume system resources. The recommended load is no more than 1-2 RPS.

{% endnote %}
