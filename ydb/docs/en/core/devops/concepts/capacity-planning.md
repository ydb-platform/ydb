# Storage capacity planning

Storage capacity planning is determining the required number of physical disks to store a given volume of data, taking into account [replication](../../concepts/topology.md) and overhead, as well as the inverse problem: estimating the useful data volume for a given amount of hardware.

## Distributed storage model {#storage-model}

At the hardware level, a cluster {{ ydb-short-name }} includes a certain number of physical disks (HDD / SSD / NVMe). Other components (CPU, RAM, network) are not covered in this article.

Clusters {{ ydb-short-name }} are multi-tenant, and one physical disk can be shared by multiple consumers — isolated databases. On top of each physical disk, one component runs to work with it — [PDisk](../../concepts/glossary.md#pdisk). The resources of one PDisk (IOPS, read and write bandwidth, capacity) are divided into equal portions called [slots](../../concepts/glossary.md#slot). Slots are reserved to create logical (virtual) disks [VDisk](../../concepts/glossary.md#vdisk). Each VDisk is assigned one slot.

To manage resources, the user has access to the entity [storage group](../../concepts/glossary.md#storage-group). A storage group is a set of several VDisks on different servers that provide fault tolerance similar to RAID. The number of VDisks in a storage group is determined by the cluster operating mode:

| Mode | Data centers | Racks in a data center<br/>(minimum) | VDisks in a storage group | Redundancy factor `RF` |
| --- | --- | --- | --- | --- |
| `block-4-2` | 1 | 8\* (10+ recommended) | 8 | 1.5 |
| `mirror-3-dc` | 3 | 3\* (4+ recommended) | 9 | 3 (up to 6\*\*) |
| `mirror-3of4` | 1 | 8\* (10+ recommended) | 8 | 4 |
| `none` | 1 | 1 | 1 | 1 |

\* The values of 8 for `block-4-2` and `mirror-3of4`, as well as 3 for `mirror-3-dc`, are the minimum required number of racks to ensure cluster operability. With this configuration, the cluster starts and runs, but has no margin for self-healing: if a rack fails, its VDisks cannot be relocated within the failure model, and any additional failure may lead to data unavailability. Additional racks are needed by the [SelfHeal](../../maintenance/manual/selfheal.md) mechanism to move VDisks from a failed rack to the remaining hardware while preserving fault tolerance guarantees. Therefore, for practical operation, it is recommended to use at least 10 racks for `block-4-2` and `mirror-3of4`, and at least 4 racks for `mirror-3-dc`. For more details on storage modes and fault tolerance guarantees, see  [{#T}](../../concepts/topology.md).

\*\* For the `mirror-3-dc` mode, under normal operation `RF = 3` (3 data copies). If one data center is unavailable, writes go to 4 copies (2 in each of the remaining data centers), and the load on the remaining data centers doubles. If a data center may be unavailable for a long time (or data centers are taken out for maintenance one by one), `RF = 6` must be factored in. If only short-term failures are acceptable (recovery is faster than the system manages to rewrite a significant portion of data), `RF = 3` is sufficient. For more details on storage modes and fault tolerance guarantees, see  [{#T}](../../concepts/topology.md).

From the user's perspective, a storage group can be perceived as a fixed set of resources: IOPS, read and write bandwidth, capacity. Each database owns one or more storage groups. Each storage group belongs to one specific database.

User tables consist of [tablets](../../concepts/glossary.md#tablet). Each tablet manages a fragment of user or system data. Tablets store data in groups; each tablet can write data to multiple storage groups, and many tablets write to each storage group.

## Values used {#metrics}

**Tablet Storage** is the total volume of data stored by the database's tablets.

**Database Storage** is the total size allocated by the database's VDisks. This size includes:

- the actual data written by tablets;
- redundancy from replication or erasure coding;
- service metadata and logs;
- allocated but not actually used space.

For existing databases, the Tablet Storage and Database Storage values can be viewed in the {{ ydb-short-name }} UI on the database page under the Info → Storage tab.

Distributed storage implements complex deferred garbage collection, so at any given moment, part of the occupied space is stale data not yet removed by the collector.

Other overheads can vary significantly depending on the nature of the stored data and the workload. For a rough estimate, `Overhead = 2` can be assumed — this is the median value obtained empirically on a sample of production databases in `mirror-3-dc` mode. Depending on the mode, data structure, and workload nature, overheads can differ significantly. For a more accurate estimate, a pilot experiment is recommended.

These values can be approximately related through the ratio:

```text
Database Storage = Tablet Storage × RF × Overhead
```

In all calculations:
1 KB = 1000 bytes, 1 MB = 1000 KB, 1 GB = 1000 MB, 1 TB = 1000 GB;
1 KiB = 1024 bytes, 1 MiB = 1024 KiB, 1 GiB = 1024 MiB, 1 TiB = 1024 GiB.

## Estimating required hardware {#hardware-estimation}

The task boils down to selecting a cluster configuration so that data is guaranteed to fit into storage groups and the cluster retains a working margin of empty slots to ensure fault tolerance.

**Input parameters:**

- `Tablet Storage` — the useful volume of data to be placed.
- `DriveSize` — the capacity of the disks used.

**Target parameters (cluster configuration):**

- `NumRacks` — the number of racks / servers ([failure domains](../../concepts/glossary.md#fail-domain))
- `DisksPerRack` — the number of disks in each failure domain.

**Reference values:**

- `RF` — redundancy factor, determined by the cluster operating mode (see [table above](#storage-model)).
- `Overhead` — storage overhead (see [section above](#metrics)); for a rough estimate, take `2`.
- `ExpectedSlotCount` — the number of slots per PDisk; determined by disk characteristics — IOPS and read and write bandwidth. Typical values: `8` for HDD, `16` for SSD/NVMe.
- `VDisksInGroup` — the number of VDisks in a storage group, determined by the cluster operating mode (see [table above](#storage-model)).

To estimate the required hardware, follow these steps.

1. Estimate the required Database Storage volume:

    ```text
    Database Storage = Tablet Storage × RF × Overhead
    ```

2. Determine the slot size:

    ```text
    SlotSize = (DriveSize - 28.08 GB) / ExpectedSlotCount
    ```

    <!-- 1 (SysLog) + 5 (SysReserveSize) + 200 (MaxCommonLogChunks) = -->
    <!-- 206 chunks (130 MiB each: 128 MiB payload + 2 MiB overhead) ≈ 28.08 GB -->

    About 28.08 GB of PDisk capacity is reserved for system needs, and the remaining space is distributed equally among slots.

    This formula is applicable for disks with a capacity of approximately 839 GB (800 GiB) or more. Using smaller disks is not recommended if optimal performance is required. For more details on disk subsystem requirements, read the section [{#T}](system-requirements.md).

3. Estimate the number of storage groups:

    ```text
    TotalGroups = ceil( Database Storage / (SlotSize × VDisksInGroup × 0.85) )
    ```

    The coefficient 0.85 reflects the recommended VDisk fill percentage (VDisk Raw Usage = 85%).

    When monitoring, it is recommended to focus on the VDisk Slot Usage and Capacity Alert values. For existing groups, the VDisk Raw Usage, VDisk Slot Usage, and Capacity Alert values can be viewed in the {{ ydb-short-name }} UI on the **Info → Storage** tab of the database page.

    The threshold at which a group is considered full corresponds to VDisk Slot Usage = 100%, Capacity Alert = LightYellow, VDisk Raw Usage ≈ 90% — while the formula above leaves a margin to this threshold.

4. Estimate the number of used slots:

    ```text
    UsedSlots = TotalGroups × VDisksInGroup
    ```

5. Obtain a lower bound for the required number of physical disks (without a fault tolerance margin):

    ```text
    TotalPDisks ≥ ceil( UsedSlots / ExpectedSlotCount )
    ```

6. Select a cluster configuration — parameters `NumRacks`, `DisksPerRack`, and determine the minimum required total margin of empty slots in the cluster:

    ```text
    TotalSlots    = NumRacks × DisksPerRack × ExpectedSlotCount
    MinEmptySlots = ceil( MaxSlotsInRack + 0.027 × TotalSlots )
    ```

    The margin of empty slots is necessary for the normal operation of the [SelfHeal](../../maintenance/manual/selfheal.md) mechanism, which performs automatic reconfiguration of storage groups to replace failed or long-unavailable disks.

    The first term `MaxSlotsInRack` is the maximum number of slots in one failure domain. For a homogeneous cluster, `MaxSlotsInRack = DisksPerRack × ExpectedSlotCount`; for a heterogeneous one, `MaxSlotsInRack = max_i(DisksPerRack_i × ExpectedSlotCount)`. This margin is needed so that if the most capacious domain fails, its VDisks can fit on the remaining hardware.

    The second term `0.027 × TotalSlots` is an empirically selected operational margin (~1 disk per 37), covering unscheduled replacement of individual disks.

7. Check that the configuration meets the minimum requirements:

    ```text
    EmptySlots = TotalSlots - UsedSlots
    ```

    The condition `EmptySlots ≥ MinEmptySlots` must hold. If the condition is not met, increase `NumRacks` or `DisksPerRack` and return to step 6. See  [{#T}](#example).

## Estimating useful storage capacity {#usable-capacity}

The inverse problem is to estimate how much useful data volume can be placed on a given amount of hardware.

Suppose there are `TotalPDisks` physical disks with a capacity of `DriveSize` each. Then:

1. Total number of slots:

    ```text
    TotalSlots = TotalPDisks × ExpectedSlotCount
    ```

2. Some slots remain empty as a margin for SelfHeal, while the rest are available for storage groups:

    ```text
    MinEmptySlots = ceil( MaxSlotsInRack + 0.027 × TotalSlots )
    UsableSlots = TotalSlots - MinEmptySlots
    ```

    Here, `MaxSlotsInRack` is the maximum number of slots in one failure domain (rack or server): for a homogeneous cluster, `DisksPerRack × ExpectedSlotCount`; for a heterogeneous one, `max_i(DisksPerRack_i × ExpectedSlotCount)`.

3. Number of storage groups:

    ```text
    TotalGroups = floor( UsableSlots / VDisksInGroup )
    ```

4. Useful capacity of the distributed storage, taking into account the 85% fill threshold:

    ```text
    Database Storage = TotalGroups × VDisksInGroup × SlotSize × 0.85
    ```

5. Estimate of useful data volume:

    ```text
    Tablet Storage = Database Storage / (RF × Overhead)
    ```

## Calculation example {#example}

Suppose it is necessary to place 100 TB of data (Tablet Storage) in a cluster with the `block-4-2` fault tolerance mode, using SSD disks with a capacity of 3.2 TB.

**Initial parameters:**

| Parameter | Value |
| --- | --- |
| Tablet Storage | 100,000 GB |
| Mode | `block-4-2` |
| RF | 1.5 |
| Overhead | 2 |
| DriveSize | 3,200 GB |
| ExpectedSlotCount | 16 |
| VDisksInGroup | 8 |

**Calculation:**

1. Database Storage = 100,000 × 1.5 × 2 = **300,000 GB**
2. SlotSize = (3,200 − 28.08) / 16 ≈ **198.25 GB**
3. TotalGroups = ⌈300,000 / (198.25 × 8 × 0.85)⌉ = **223 storage groups**
4. UsedSlots = 223 × 8 = **1,784 slots**
5. TotalPDisks (without margin) = ⌈1,784 / 16⌉ = **112 disks**
6. Next, we select a specific cluster configuration, providing a margin of empty slots to ensure fault tolerance. To do this, we consider various hardware options and check compliance with minimum requirements, taking into account that the `block-4-2` mode requires at least 8 failure domains (servers or racks), and 10 or more are recommended for stable practical operation:

    - Let's take 10 racks with 12 disks each. One rack has 12 × 16 = 192 slots, TotalSlots = 1,920. MinEmptySlots = ⌈192 + 0.027 × 1,920⌉ = 244. EmptySlots = 1,920 − 1,784 = 136. 136 < 244 — minimum requirements are not met. It is easy to verify that 10 racks with 14 disks each are sufficient: TotalSlots = 2,240, MinEmptySlots = ⌈224 + 0.027 × 2,240⌉ = 285, EmptySlots = 2,240 − 1,784 = 456 > 285.

    - A server can be chosen as the failure domain, and 30 servers with 4 disks each can be considered. One failure domain has 4 × 16 = 64 slots, TotalSlots = 1,920. MinEmptySlots = ⌈64 + 0.027 × 1,920⌉ = 116. EmptySlots = 1,920 − 1,784 = 136 > 116 — minimum requirements are met.

Thus, to store 100 TB of data in `block-4-2` mode on SSD disks with a capacity of 3.2 TB, 10 racks with 14 disks each (140 disks) or 30 servers with 4 disks each (120 disks) will be required.

**Reverse calculation:**

Next, we take the configuration from the previous example — 30 servers with 4 SSD disks each with a capacity of 3.2 TB, and estimate how much useful data volume can fit in such a cluster.

1. TotalSlots = 120 × 16 = **1,920 slots**
2. MinEmptySlots = ⌈MaxSlotsInRack + 0.027 × TotalSlots⌉ = ⌈4 × 16 + 0.027 × 1,920⌉ = ⌈115.84⌉ = **116 slots**
3. UsableSlots = 1,920 − 116 = **1,804 slots**
4. TotalGroups = ⌊1,804 / 8⌋ = **225 storage groups**
5. SlotSize = (3,200 − 28.08) / 16 ≈ **198.25 GB**
6. Database Storage = 225 × 8 × 198.25 × 0.85 ≈ **303,323 GB**
7. Tablet Storage = 303,323 / (1.5 × 2) ≈ **101,108 GB**

Thus, a cluster of 30 servers and 120 SSD disks of 3.2 TB each in `block-4-2` mode can accommodate about 101.1 TB of data (Tablet Storage).

## Additional information {#see-also}

* [{#T}](../../concepts/topology.md)
* [{#T}](system-requirements.md)
* [{#T}](../../maintenance/manual/selfheal.md)
