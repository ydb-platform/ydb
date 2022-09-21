## Description of the BlobStorage interface

### Blob ID format

Each blob has a 192-bit ID consisting of the following fields (in the order used for sorting):

1. TabletId (64 bits): ID of the blob owner tablet.
2. Channel (8 bits): Channel sequence number.
3. Generation (32 bits): Generation in which the tablet that captured this blob was run.
4. Step (32 bits): Blob group internal ID within Generation.
5. Cookie (24 bits): ID to use if Step is insufficient.
6. CrcMode (2 bits): Selects a mode for redundant blob integrity verification at the BlobStorage level.
7. BlobSize (26 bits): Blob data size.
8. PartId (4 bits): Fragment number when using blob erasure coding. At the "BlobStorage <-> tablet" communication level, this parameter is always 0 referring to the entire blob.

Two blobs are considered different if at least one of the first five parameters (TabletId, Channel, Generation, Step, or Cookie) differs in their IDs. So it is impossible to write two blobs that only differ in BlobSize and/or CrcMode.

For debugging purposes, there is string blob ID representation in `[TabletId:Generation:Step:Channel:Cookie:BlobSize:PartId]` format, for example, `[12345:1:1:0:0:1000:0]`.

When writing a blob, the tablet selects the Channel, Step, and Cookie parameters. TabletId is fixed and must point to the tablet performing the write operation, while Generation must indicate the generation that the tablet performing the operation is running in.

When performing reads, the blob ID is specified, which can be arbitrary, but preferably preset.

### Groups

Blobs are written in a logical entity called *group*. A special actor called DS proxy is created on every node for each group that is written to. This actor is responsible for performing all operations related to the group. The actor is created automatically through the NodeWarden service that will be described below.

Physically, a group is a set of multiple physical devices (OS block devices) that are located on different nodes so that the failure of one device correlates as little as possible with the failure of another device. These devices are usually located in different racks or different datacenters. On each of these devices, some space is allocated for the group, which is managed by a special service called *VDisk*. Each VDisk runs on top of a block storage device from which it is separated by another service called *PDisk*. Blobs are broken into fragments based on [erasure coding](https://en.wikipedia.org/wiki/Erasure_code) with these fragments written to VDisks. Before splitting into fragments, optional encryption of the data in the group can be performed.

This scheme is shown in the figure below.

![PDisk, VDisk, and a group](../../_assets/Slide3_group_layout.svg)

VDisks from different groups are shown as multicolored squares; one color stands for one group.

A group can be treated as a set of VDisks:

![Group](../../_assets/Slide_group_content.svg)

Each VDisk within a group has a sequence number, and disks are numbered 0 to N-1, where N is the number of disks in the group.

In addition, the group disks are grouped into fail domains and fail domains into fail realms. Each fail domain usually has exactly one disk inside (although, in theory, it may have more, but this is not used in practice), while multiple fail realms are only used for groups whose data is stored in all three data centers. Thus, in addition to a group sequence number, each VDisk is assigned an ID that consists of a fail realm index, the index that a fail domain has in a fail realm, and the index that a VDisk has in the fail domain. In string form, this ID is written as `VDISK[GroupId:GroupGeneration:FailRealm:FailDomain:VDisk]`.

All fail realms have the same number of fail domains, and all fail domains include the same number of disks. The number of the fail realms, the number of the fail domains inside the fail realm, and the number of the disks inside the fail domain make up the geometry of the group. The geometry depends on the way the data is encoded in the group. For example, for block-4-2 numFailRealms = 1, numFailDomainsInFailRealm >= 8 (only 8 fail realms are used in practice), numVDisksInFailDomain >= 1 (strictly 1 fail domain is used in practice). For mirror-3-dc numFailRealms >= 3, numFailDomainsInFailRealm >= 3, and numVDisksInFailDomain >= 1 (3x3x1 are used).

Each PDisk has an ID that consists of the number of the node that it is running on and the internal number of the PDisk inside this node. This ID is usually written as NodeId:PDiskId. For example, 1:1000. If you know the PDisk ID, you can calculate the service ActorId of this disk and send it a message.

Each VDisk runs on top a specific PDisk and has a *slot ID* comprising three fields (NodeID:PDiskId:VSlotId) as well as the above-mentioned VDisk ID. Strictly speaking, there are different concepts: a slot is a reserved location on a PDISK occupied by a VDisk while a VDisk is an element of a group that occupies a certain slot and performs operations with the slot. Similarly to PDisks, if you know the slot ID, you can calculate the service ActorId of the running VDisk and send it a message. To send messages from the DS proxy to the VDisk, an intermediate actor called *BS_QUEUE* is used.

The composition of each group is not constant. It may change while the system is running. Hence the concept of a group generation. Each "GroupId:GroupGeneration" pair corresponds to a fixed set of slots (a vector that consists of N slot IDs, where N is equal to group size) that stores the data of an entire group. *Group generation is not to be confused with tablet generation since they are not in any way related*.

As a rule, groups of two adjacent generations differ by no more than one slot.

### Subgroups

A special concept of a *subgroup* is introduced for each blob. It is an ordered subset of group disks with a strictly constant number of elements that will store the blob's data and that depends on the encoding type (the number of elements in a group must be the same or greater). For single-datacenter groups with conventional encoding, this subset is selected as the first N elements of a cyclic disk permutation in the group, where the permutation depends on the BlobId hash.

Each disk in the subgroup corresponds to a disk in the group, but is limited by the allowed number of stored blobs. For example, for block-4-2 encoding with four data parts and two parity parts, the functional purpose of the disks in a subgroup is as follows:

| Number in the subgroup | Possible PartIds |
|-------------------|-------------------|
| 0 | 1 |
| 1 | 2 |
| 2 | 3 |
| 3 | 4 |
| 4 | 5 |
| 5 | 6 |
| 6 | 1,2,3,4,5,6 |
| 7 | 1,2,3,4,5,6 |


In this case, PartId=1..4 corresponds to data fragments (resulting from dividing the original blob into 4 equal parts), while PartId=5..6 stands for parity fragments. Disks numbered 6 and 7 in the subgroup are called *handoff disks*. Any part, either one or more, can be written to them. You can only write the respective blob parts to disks 0..5.

In practice, when performing writes, the system tries to write 6 parts to the first 6 disks of the subgroup and, in the vast majority of cases, these attempts are successful. However, if any of the disks is not available, a write operation cannot succeed, which is when handoff disks kick in receiving the parts belonging to the disks that did not respond in time. It may turn out that several fragments of the same blob are sent to the same handoff disk as a result of complicated brakes and races. This is acceptable although it makes no sense in terms of storage: each fragment must have its own unique disk.
