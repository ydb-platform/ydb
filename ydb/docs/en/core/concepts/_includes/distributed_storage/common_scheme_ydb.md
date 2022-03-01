## General YDB schema

An approximate general YDB schema is shown below.

![General schema](../../_assets/BS_overview.svg)

### Nodes

A single YDB installation consists of a *cluster* that is divided into *nodes*. A node is a single process in the system, usually kikimr. This node is part of a cluster and can exchange data with its other nodes via *Interconnect*. Each *node* has its own ID which is usually named NodeId. NodeID is an integer from 1, consisting of 20 bits. NodeID 0 is reserved for internal needs and usually indicates the current node or no node.

A number of services are run on each node and implemented via *actors*.

Nodes can be static and dynamic.

A configuration of static nodes, that is, their complete list with the address for connecting via Interconnect specified, is stored in a configuration file and is read once when the process is started. A set of static nodes changes very rarely. This usually happens when expanding clusters or moving nodes from one physical machine to another. To change the set of static nodes, apply the updated configuration to **every** node and then perform a rolling restart of the entire cluster.

Dynamic nodes are not known in advance and are added to the system as new processes are started. This may be due, for example, to the creation of new tenants in YDB installations as a database. When registering a dynamic node, its process first connects to one of the static nodes via gRPC, transmits information about itself through a special service called Node Broker, and receives the NodeID to log in with in response. The mechanism for assigning nodes is pretty much similar to the DHCP in the context of distributing IP addresses.

### Tablets

Special microservices called *tablets* are run on each node. Each tablet has a specific type and ID and is a singleton, meaning that only one tablet with a specific ID can be running in the entire cluster at any given time. A tablet can be started on any node that is suitable for it. An important characteristic of a tablet is its *generation* that increases with each subsequent run. Please note that due to the distributed nature of the system and in case of various kinds of issues, such as with network partitioning, a situation may arise when the same tablet will actually be running on two different nodes at the same time. However, BlobStorage guarantees that only one of them will be able to successfully complete operations that change its state and the generation that each successful operation is performed in will not decrease over time.

You can find out on what node the tablet in the current generation is running through the *StateStorage* service. To send messages to tablets, use a special set of libraries named *tablet pipe*. With it, knowing the ID of the target tablet, you can easily send the desired message to it.

A tablet can be divided into two parts: the basic tablet and the user logic.

The basic tablet is a set of tables, each of which may consist of one or more key columns of an arbitrary type and a set of data columns. Each table may have its own schema. In addition, tables can be created and deleted while the tablet is running. The interface of the basic tablet lets you perform read and update operations on these tables.

The user logic is located between the basic tablet and the user and lets you process specific requests for this type of tablet, reliably saving changes in BlobStorage. A commonly used template for tablet operation is storing all data in memory, reading it only at the start, and synchronously changing the data in memory and in storage after a successful commit.

#### How does a tablet store data and what data is it?

The basic tablet is an LSM tree that contains all of its table data. One level below the basic tablet is BlobStorage that, roughly speaking, is KeyValue storage that stores binary large objects (blobs). *BLOB* is a binary fragment from 1 byte to 10 MB in size, which has a fixed ID (that is usually called *BlobId* and is of the TLogoBlobID type) and contains related data. Storage is immutable, meaning that only one value corresponds to each ID and it cannot change over time. You can write and read a blob and then delete it when you no longer need it.

For BlobStorage, blobs are an opaque entity. A tablet can store several types of blobs. The most frequently written blob is a log blob (meaning a recovery log). A tablet's log is arranged in a list of blobs, each of which contains information about the change being made to the tables. When run, the tablet finds the last blob in the log and then recursively reads all related blobs following the links. The log may also mention snapshot blobs, which is a type of blobs that contain data from multiple log blobs after merging them (the merge operation in the LSM tree).

The tablet writes blobs of different types to different *channels*. A channel specifies the branch of storage to store blobs in and performs various functions, such as:

1. Choosing the storage type (different channels can be linked to different types of storage devices: SSD, HDD, or NVME).
1. Load balancing, because each channel has a limit on IOPS, available space and bandwidth.
1. Specifying the data type. When restoring the log, only the blobs from the null channel are read, which lets you distinguish them from other blobs.

#### Blob ID format

Each blob has a 192-bit ID consisting of the following fields (in the order used for sorting):

1. TabletId (64 bits): ID of the blob owner tablet.
1. Channel (8 bits): Channel sequence number.
1. Generation (32 bits): Number of the generation in which the tablet that wrote this blob was run.
1. Step (32 bits): Internal number of the blob group within the Generation.
1. Cookie (24 bits): ID that can be used if the Step is insufficient.
1. CrcMode (2 bits): Selects mode for redundant blob integrity control at the BlobStorage level.
1. BlobSize (26 bits): Blob data size.
1. PartId (4 bits): Part number when blob erasure coding is used. At the "BlobStorage <-> tablet" interaction level, this parameter is always 0, meaning the entire blob.

Two blobs are considered different if at least one of the first five parameters (TabletId, Channel, Generation, Step, or Cookie) differs in their IDs. So it is impossible to write two blobs that only differ in BlobSize and/or CrcMode.

For debugging purposes, there is string blob ID formatting that has interactions `[TabletId:Generation:Step:Channel:Cookie:BlobSize:PartId]`, for example, `[12345:1:1:0:0:1000:0]`.

When writing a blob, the tablet selects the Channel, Step, and Cookie parameters. The TabletId is fixed and must indicate the tablet performing the write operation, while the Generation parameter must indicate the generation that the tablet performing the operation is running in.

When performing reads, the blob ID is specified, which can be arbitrary, but preferably preset.

#### Groups

Blobs are written in a logical entity called *group*. On each node, a special actor called DS proxy is created for each group that the blob is written to. This actor is responsible for performing all operations related to the group. The actor is created automatically through the NodeWarden service that will be described below.

Physically, a group is a set of multiple physical devices (OS block devices) that are located on different nodes so that the failure of one device correlates as little as possible with the failure of another device. These devices are usually located in different racks or different datacenters. On each of these devices, some space is allocated for the group, which is managed by a special service called *VDisk*. Each VDisk runs on top of a block device from which it is separated by another service called *PDisk*. Blobs are broken into fragments based on *erasure coding* with these fragments written to VDisks. Before splitting into fragments, optional encryption of the data in the group can be performed.

This scheme is shown in the figure below.

![PDisk, VDisk, and a group](../../_assets/Slide3_group_layout.svg)

VDisks from different groups are shown as multicolored squares; one color stands for one group.

A group can be treated as a set of VDisks:

![Group](../../_assets/Slide_group_content.svg)

Each VDisk within the group has a sequence number, the disks are numbered from 0 to N-1, where N is the number of disks in the group.

In addition, the group disks are combined into fail domains, while the fail domains are combined into fail realms. As a rule, each fail domain has exactly one disk inside (although, in theory, it may have more, but this has found no application in practice), and multiple fail realms are only used for groups that host their data in three datacenters at once. In addition to the sequence number in the group, each VDisk is assigned an ID that consists of a fail realm index, fail domain index inside the fail realm, and the VDisk index inside the fail domain. In string form, this ID is written as `VDISK[GroupId:GroupGeneration:FailRealm:FailDomain:VDisk]`.

All the fail realms have the same number of fail domains and all the fail domains have the same number of disks inside. The number of the fail realms, the number of the fail domains inside the fail realm, and the number of the disks inside the fail domain make up the geometry of the group. The geometry depends on the way the data is encoded in the group. For example, for block-4-2 numFailRealms = 1, numFailDomainsInFailRealm >= 8 (only 8 fail realms are used in practice), numVDisksInFailDomain >= 1 (strictly 1 fail domain is used in practice). For mirror-3-dc numFailRealms >= 3, numFailDomainsInFailRealm >= 3, and numVDisksInFailDomain >= 1 (3x3x1 are used).

Each PDisk has an ID that consists of the number of the node that it is running on and the internal number of the PDisk inside this node. This ID is usually written as NodeId:PDiskId. For example, 1:1000. If you know the PDisk ID, you can calculate the service ActorId of this disk and send it a message.

Each VDisk runs on top of a specific PDisk and has a *slot ID* consisting of three fields (NodeID:PDiskId:VSlotId) and the above-mentioned VDisk ID. Strictly speaking, there are different concepts: a slot is the space reserved on the PDisk and occupied by the VDisk, and the VDisk is a group component that occupies a certain slot and performs operations on it. Similarly to PDisks, if you know the slot ID, you can calculate the service ActorId of the running VDisk and send it a message. To send messages from the DS proxy to the VDisk, an intermediate actor called *BS_QUEUE* is used.

The composition of each group is not fixed: it may change while using the system. For this purpose, the concept of "group generation" is introduced. Each "GroupId:GroupGeneration" pair corresponds to a fixed set of slots (a vector that consists of N slot IDs, where N is the size of the group), storing the data of the entire group. *Please note that a group generation and a tablet generation are not related in any way*.

As a rule, groups of two adjacent generations differ by no more than one slot.

#### Subgroups

For each blob, a special concept of a *subgroup* is introduced, which is an ordered subset of group disks with a strictly fixed number of elements, depending on the type of encoding (the number of elements in the group must be at least the same), where the data of this blob will be stored. For single-datacenter groups with conventional encoding, this subset is selected as the first N elements of a cyclic disk permutation in the group, where the permutation depends on the BlobId hash.

Each disk in the subgroup corresponds to a disk in the group, but is limited by the allowed number of stored blobs. For example, to encode block-4-2 with four data parts and two parity parts, the functional purpose of disks in the subgroup is as follows:

| Number in the subgroup | Possible PartIds |
| ------------------- | ------------------- |
| 0 | 1 |
| 1 | 2 |
| 2 | 3 |
| 3 | 4 |
| 4 | 5 |
| 5 | 6 |
| 6 | 1,2,3,4,5,6 |
| 7 | 1,2,3,4,5,6 |

In this case, PartID=1..4 corresponds to the data parts (which are obtained by splitting the original blob into 4 equal parts), and PartID=5..6 are parity parts. Disks numbered 6 and 7 in the subgroup are called *handoff disks*. Any part, either one or more, can be written to them. Disks 0..5 can only store the corresponding blob parts.

In practice, when performing writes, the system tries to write 6 parts to the first 6 disks of the subgroup and, in the vast majority of cases, these attempts are successful. However, if any of the disks is not available, the write operation fails and the system uses handoff disks where parts of the disks that did not respond in time are sent. This may result in a situation when multiple parts of the same blob are written to one handoff disk. This is acceptable, although it makes no sense in terms of storage: each part must have its own unique disk.

#### Tablet channel history

As mentioned above, each group has a fixed amount of data that can fit into it and also divides the bandwidth by throughput and the number of operations per second between all consumers. The load on tablets may vary. As a result a group may become overloaded. In this regard, the concept of history is introduced, with which, for each tablet, knowing a blob's Channel and Generation, you can determine the group that this blob is written to.

This mechanism works as follows:

![Channel history](../../_assets/Slide_blob.svg)

For each channel, the TTabletStorageInfo structure contains the TTabletChannelInfo substructure with generation ranges and the group number corresponding to each range. The ranges are strictly adjacent to each other, the last range is open. Group numbers may overlap in different ranges and even across different channels: this is not prohibited and is quite common.

When writing a blob, a tablet selects the most recent range for the corresponding channel, since the write operation is always performed on behalf of the current generation of the tablet. When reading a blob, the group number is fetched based on the BlobId.Generation of the blob being read.

#### Garbage collection

#### Tablet suspension
