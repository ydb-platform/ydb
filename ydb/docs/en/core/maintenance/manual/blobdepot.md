# Blobovnica

Blobovnica extends the functionality of the storage subsystem by adding virtual group functionality.

A virtual group, along with a physical group, is a unit of fault tolerance of the storage subsystem in a cluster, but a virtual group stores its data in other groups (unlike a physical group, which stores data on VDisks).

This method of data storage makes it possible to use the storage subsystem more flexibly {{ ydb-name }}, in particular:

* use "heavier" tablets in conditions where the size of one physical group is limited;
* provide tablets with a wider write bandwidth by balancing writes across all groups on top of which blobovnica is running;
* ensure transparent migration of data between different groups for client tablets.

Blobovnica can also be used for group decommissioning, that is, for removing a VDisk inside a physical group while preserving all data that was written to that group. In this usage scenario, data from the physical group is transparently moved to the virtual group for the client, then all VDisks of the physical group are removed to free up the resources they occupy.

## Virtual group mode

In virtual group mode, blobovnica allows combining multiple groups into a single space for storing large amounts of data. Balancing by occupied space is provided, as well as increased throughput by distributing writes across different groups. Background (and completely transparent to the client) data transfer is also possible.

Virtual groups are also created inside Storage Pools, like physical groups, but for virtual groups it is recommended to create a separate pool using the command `dstool pool create virtual`. These pools can be specified in Hive to create other tablets. However, to avoid latency degradation, it is recommended to place tablet channels 0 and 1 on physical groups, and place only data channels on virtual groups with blobovnica.

### How to run {#vg-create-params}

A virtual group is created through the BS\_CONTROLLER by sending a special command. The command to create a virtual group is idempotent, so to avoid creating extra blobovnicas, each virtual group is assigned a name. The name must be unique within the entire cluster. If the command is executed again, an error will be returned with the field filled in `Already: true` and an indication of the number of the previously created virtual group.

```bash
dstool -e ... --direct group virtual create --name vg1 vg2 --hive-id=72057594037968897 --storage-pool-name=/Root:virtual --log-channel-sp=/Root:ssd --data-channel-sp=/Root:ssd*8
```

Command line parameters:

* --name unique name for the virtual group (or several virtual groups with similar parameters);
* --hive-id=N number of the Hive tablet that will manage this blobovnica; you must specify the Hive of the tenant within which blobovnica is running;
* --storage-pool-name=POOL\_NAME is the name of the Storage Pool in which the blob storage must be created;
* --storage-pool-id=BOX:POOL is an alternative `--storage-pool-name`in which you can specify an explicit numeric pool identifier;
* --log-channel-sp=POOL\_NAME is the name of the pool in which channel 0 of the blob storage tablet will be placed;
* --snapshot-channel-sp=POOL\_NAME is the name of the pool in which channel 0 of the blob storage tablet will be placed; if not specified, the value from --log-channel-sp is used;
* --data-channel-sp=POOL\_NAME[\*COUNT] is the name of the pool in which data channels are placed; if the COUNT parameter is specified (after the "asterisk" sign), then COUNT data channels are created in the specified pool; it is recommended to create a large number of data channels for the blob storage in virtual group mode (64..250) to use the storage most efficiently;
* --wait wait for the creation of blob storages to complete; if this option is not specified, the command completes immediately after responding to the request to create the blob storage, without waiting for the creation and startup of the tablets themselves.

### How to check that everything has started {#vg-check-running}

You can view the result of creating a virtual group in the following ways:

* via the BS\_CONTROLLER monitoring page;
* via the command `dstool group list --virtual-groups-only`.

In both cases, you need to monitor the creation via the VirtualGroupName field, which must match what was passed in the --name parameter. If the command `dstool group virtual create` completed successfully, the virtual group unconditionally appears in the list of groups, but the VirtualGroupState field can take one of the following values:

* NEW — the group is waiting for initialization (the tablet is being created via Hive, configured, and started);
* WORKING — the group is created and running, ready to handle user requests;
* CREATE\_FAILED — an error occurred during group creation, a text description of which can be seen in the ErrorReason field.

```bash
$ dstool --cluster=$CLUSTER --direct group list --virtual-groups-only
┌────────────┬──────────────┬───────────────┬────────────┬────────────────┬─────────────────┬──────────────┬───────────────────┬──────────────────┬───────────────────┬─────────────┬────────────────┐
│ GroupId    │ BoxId:PoolId │ PoolName      │ Generation │ ErasureSpecies │ OperatingStatus │ VDisks_TOTAL │ VirtualGroupState │ VirtualGroupName │ BlobDepotId       │ ErrorReason │ DecommitStatus │
├────────────┼──────────────┼───────────────┼────────────┼────────────────┼─────────────────┼──────────────┼───────────────────┼──────────────────┼───────────────────┼─────────────┼────────────────┤
│ 4261412864 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg1              │ 72075186224037888 │             │ NONE           │
│ 4261412865 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg2              │ 72075186224037890 │             │ NONE           │
│ 4261412866 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg3              │ 72075186224037889 │             │ NONE           │
│ 4261412867 │ [1:2]        │ /Root:virtual │ 0          │ none           │ DISINTEGRATED   │ 0            │ WORKING           │ vg4              │ 72075186224037891 │             │ NONE           │
└────────────┴──────────────┴───────────────┴────────────┴────────────────┴─────────────────┴──────────────┴───────────────────┴──────────────────┴───────────────────┴─────────────┴────────────────┘
```

## Structure

The blob storage is a tablet that, in addition to the two system channels (0 and 1), also contains a set of additional channels that store the data itself written to the blob storage. Client data is written to these additional channels.

The blob storage as a tablet can be run on any cluster node.

When the blob storage operates in virtual group mode, agents (BlobDepotAgent) are used to access it. These are actors that perform functions similar to a DS proxy — they run on each node that uses the virtual group with the blob storage. These same actors convert storage requests into commands for the blob storage and provide data exchange with it.

## Diagnostic mechanisms

The following set of mechanisms is provided for diagnosing the health of the blob storage:

* [BS monitoring page\_CONTROLLER](#diag-bscontroller);
* [blob storage monitoring page](#diag-blobdepot);
* [internal viewer](#diag-viewer);
* [event log](#diag-log);
* [graphs](#diag-sensors).

### BS monitoring page\_CONTROLLER {#diag-bscontroller}

On the BS monitoring page\_CONTROLLER there is a special tab called Virtual groups that shows all groups that use the blob storage:

![Virtual groups](_assets/virtual-groups.png "Virtual groups")

The table has the following columns:

Field | Description
---- | --------
GroupId | Group number.
StoragePoolName | Name of the pool where the group resides.
Name | Name of the virtual group; it is unique across the entire cluster. For decommissioned groups, this will be null.
BlobDepotId | Number of the blob storage tablet that is responsible for serving this group.
State | [Blob storage state](#vg-check-running); can be NEW, WORKING, CREATED\_FAILED.
HiveId | Number of the Hive tablet inside which the specified blob storage was created.
ErrorReason | When the state is CREATE\_FAILED, contains a text description of the reason for the creation error.
DecommitStatus | [Group decommission state](blobdepot_decommit.md#decommit-check-running); can be NONE, PENDING, IN\_PROGRESS, DONE.

### Blob storage monitoring page {#diag-blobdepot}

The blob storage monitoring page shows the main operating parameters of the tablet, grouped into tabs available via the "Contained data" link:

* [data](#mon-data)
* [refcount](#mon-refcount)
* [trash](#mon-trash)
* [barriers](#mon-barriers)
* [blocks](#mon-blocks)
* [storage](#mon-storage)

In addition, the main page provides brief information about the state of the blob storage:

![BlobDepot stats](_assets/blobdepot-stats.png "BlobDepot stats")

The following data is provided in this table:

* Data, bytes — the number of stored data bytes ([TotalStoredDataSize](#diag-sensors)).
* Trash in flight, bytes — the number of bytes of unnecessary data that are waiting for transactions to complete before becoming garbage ([InFlightTrashSize](#diag-sensors)).
* Trash pending, bytes — the number of garbage bytes that have not yet been passed to garbage collection ([TotalStoredTrashSize](#diag-sensors)).
* Data in GroupId# XXX, bytes — the number of data bytes in group XXX (both useful data and garbage that has not yet been collected).

![BlobDepot main](_assets/blobdepot-main.png "BlobDepot main")

The purpose of the parameters is as follows:

* Loaded — a boolean value indicating whether all metadata from the tablet's local database has been loaded into memory.
* Last assimilated blob id — the BlobId of the last read blob (metadata copying during decommission).
* Data size, number of keys — the number of stored data keys.
* RefCount size, number of blobs — the number of unique data blobs that the blob storage keeps in its namespace.
* Total stored data size, bytes — similar to "Data, bytes" from the table above.
* Keys made certain, number of keys — the number of incomplete keys that were subsequently confirmed by reading.

The "Uncertainty resolver" section refers to the component that works with data written to the blob storage but not confirmed.

#### data {#mon-data}

![data tab](_assets/blobdepot-data.png "data tab")

The data table contains the following columns:

* key — key identifier (BlobId in the client namespace);
* value chain — key value formed by concatenating blob fragments from the blob storage namespace (this field lists these very blobs);
* keep state — value of keep flags for this blob as seen by the client (Default, Keep, DoNotKeep);
* barrier — field showing which barrier this blob falls under (S — under soft barrier, H — under hard barrier; in fact, H never occurs, since blobs are synchronously deleted from the table at the moment the hard barrier is set).

Given the potentially large table size, only a part of it is shown on the monitoring page. To find the desired blob, you can fill in the "seek" field by entering the BlobId of the blob you are looking for, then specify the number of rows of interest before and after this blob and click the "Show" button.

#### refcount {#mon-refcount}

![refcount tab](_assets/blobdepot-refcount.png "refcount tab")

The refcount table contains two columns: "blob id" and "refcount". Blob id is the identifier of the stored blob written on behalf of the blob storage to the storage. Refcount is the number of references to this blob from the data table (from the value chain column).

The TotalStoredDataSize metric is formed from the sum of sizes of all blobs in this table, each counted exactly once, without taking the refcount field into account.

#### trash {#mon-trash}

![trash tab](_assets/blobdepot-trash.png "trash tab")

The table contains three columns: "group id", "blob id", and "in flight". Group id is the number of the group where a no longer needed blob is stored. Blob id is the identifier of the blob itself. In flight is a flag indicating that the blob is still going through a transaction, only after the results of which it can be passed to the garbage collector.

The TotalStoredTrashSize and InFlightTrashSize metrics are formed from this table by summing the sizes of blobs without the in flight flag and with it, respectively.

#### barriers {#mon-barriers}

![barriers tab](_assets/blobdepot-barriers.png "barriers tab")

The barriers table contains information about client barriers that were passed to the blob depot. It consists of the columns "tablet id" (tablet number), "channel" (channel number for which the barrier is recorded), as well as barrier values: "soft" and "hard". The value has the format gen:counter => collect\_gen:collect\_step, where gen is the tablet generation number in which this barrier was set, counter is the sequence number of the garbage collection command, collect\_gen:collect\_step is the barrier value (all blobs whose generation and step within the generation are less than or equal to the specified barrier are deleted).

#### blocks {#mon-blocks}

![blocks tab](_assets/blobdepot-blocks.png "blocks tab")

The blocks table contains a list of locks on client tablets and consists of the columns "tablet id" (tablet number) and "blocked generation" (the generation number of this tablet in which nothing can be written anymore).

#### storage {#mon-storage}

![storage tab](_assets/blobdepot-storage.png "storage tab")


The storage table shows statistics on stored data for each group in which the blob depot stores data. This table contains the following columns:

* group id — the number of the group where the data is stored;
* bytes stored in current generation — the amount of data written to this group in the current tablet generation (only useful data is counted, excluding garbage);
* bytes stored total — the amount of all data saved by this blob depot to the specified group;
* status flag — color flags of the group status;
* free space share — an indicator of group fullness (a value of 0 corresponds to a completely full group, 1 to a completely free one).

### Internal viewer {#diag-viewer}

On the Internal viewer monitoring page shown below, blob depots can be seen in the Storage section and as BD tablets.

In the Nodes section, BD tablets running on different system nodes are visible:

![Nodes](_assets/viewer-nodes.png "Nodes")

In the Storage section, you can see virtual groups that operate through the blob depot. They can be distinguished by the link with the text BlobDepot in the Erasure column. The link in this column leads to the tablet monitoring page. Otherwise, virtual groups are displayed the same way, except that they have no PDisk and VDisk. However, decommissioned groups will look the same as virtual ones but will have PDisk and VDisk until the decommissioning is complete.

![Storage](_assets/viewer-storage.png "Storage")

### Event log {#diag-log}

The blob depot tablet writes events to the log with the following component names:

* BLOB\_DEPOT — the blob depot tablet component.
* BLOB\_DEPOT\_AGENT — the blob depot agent component.
* BLOB\_DEPOT\_TRACE — a special component for debug tracing of all events related to data.

BLOB\_DEPOT and BLOB\_DEPOT\_AGENT are output as structured records with fields that allow identifying the blob depot and the group it serves. For BLOB\_DEPOT this field is Id and has the format {TabletId:GroupId}:Generation, where TabletId is the number of the blob depot tablet, GroupId is the number of the group it serves, and Generation is the generation in which the running blob depot writes messages to the log. For BLOB\_DEPOT\_AGENT this field is called AgentId and has the format {TabletId:GroupId}.

At the DEBUG level, most occurring events will be written to the log, both on the tablet side and on the agent side. This mode is used for debugging and is not recommended in production environments due to the large number of generated events.

### Charts {#diag-sensors}

Each blob depot tablet produces the following charts:

Chart                | Type         | Description
-------------------- | ------------ | --------
TotalStoredDataSize  | simple       | The amount of stored user data net (if there are multiple references to one blob, it is counted once).
TotalStoredTrashSize | simple       | The number of bytes in garbage data that are no longer needed but have not yet been passed to garbage collection.
InFlightTrashSize    | simple       | The number of garbage bytes that are still waiting for confirmation of writing to the local database (they cannot even be started to collect yet).
BytesToDecommit      | simple       | The number of data bytes that remain to be [decommissioned](blobdepot_decommit.md#decommit-progress) (if this blob depot operates in the group decommission mode).
Puts/Incoming        | cumulative   | The rate of incoming write requests (in items per unit time).
Puts/Ok              | cumulative   | The number of successfully completed write requests.
Puts/Error           | cumulative   | The number of write requests completed with an error.
Decommit/GetBytes    | cumulative   | The rate of data reading during [decommission](blobdepot_decommit.md#decommit-progress).
Decommit/PutOkBytes  | cumulative   | The rate of data writing during [decommission](blobdepot_decommit.md#decommit-progress) (only successfully completed writes are counted).
