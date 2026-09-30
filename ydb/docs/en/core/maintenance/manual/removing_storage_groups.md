# Removing storage groups from a database

As a [database](../../concepts/glossary.md#database) evolves, it may end up with more [storage groups](../../concepts/glossary.md#storage-group) than it needs: for example, after data has been deleted, or after the database was temporarily expanded. Each storage group occupies [slots](../../concepts/glossary.md#slot) on [PDisks](../../concepts/glossary.md#pdisk), so unused groups keep cluster resources that could be given to other databases.

Removing storage groups lets you return some of the groups of a database back to the cluster. The data stored in the removed groups is moved to the remaining groups of the same [storage pool](../../concepts/glossary.md#storage-pool), after which the removed groups are deleted and their [VDisks](../../concepts/glossary.md#vdisk) and PDisk slots are freed.

{% note info %}

This procedure differs from [{#T}](virtual_storage_groups_decommit.md). Decommissioning with virtual groups replaces a physical group with a virtual one, which keeps serving the original group ID. Removing storage groups, as described in this article, makes the tablets of the database stop using the removed groups completely, so no virtual groups are created.

{% endnote %}

## How it works

Storage groups are allocated to a database in *storage units*: one storage unit of a given kind corresponds to one storage group in the database's storage pool of that kind (for example, `/Root/db1:ssd` for the `ssd` kind). To remove storage groups, you decrease the number of storage units of the database with the [CMS](../../concepts/glossary.md#cms) `AlterDatabase` request. The rest happens automatically, in the background:

1. **Changing the required number of groups.** [Console](../../concepts/glossary.md#console) saves the new required number of storage units and replies to the request right away. The database switches to the `REMOVING_STORAGE_UNITS` state.
1. **Selecting groups to remove.** Console asks [Hive](../../concepts/glossary.md#hive) to shrink the storage pool to the new size. Hive selects the groups that currently store the least amount of data and marks them as inactive. New data is no longer placed into inactive groups. If the database has its own Hive, the root Hive forwards the request to it, and both of them take part in the process.
1. **Moving tablet channels.** Every [tablet](../../concepts/glossary.md#tablet) [channel](../../concepts/glossary.md#channel) that currently writes to an inactive group is reassigned to one of the remaining groups of the pool. From this moment on, tablets write new data only to the remaining groups. Reassigning a channel restarts the tablet, which usually takes fractions of a second.
1. **Moving the data.** Old data written before the channels were reassigned is still stored in the inactive groups. Hive asks every tablet that has such data to rewrite it: the tablet [compacts](../../concepts/glossary.md#compaction) all its data into the new groups and deletes the old copies. Tablets are processed one at a time. After a tablet finishes, Hive restarts it, and the tablet stops referencing the inactive groups.
1. **Deleting the groups.** When no tablet references the inactive groups, Hive reports this to Console. Console deletes the groups in the [distributed storage](../../concepts/glossary.md#distributed-storage), which frees their VDisks and PDisk slots. The number of allocated storage units decreases, and the database returns to the `RUNNING` state.

{% note info %}

The database stays fully available during the whole process: users can read and write data as usual. However, moving the data creates additional load on the database and on the storage system: tablets compact and rewrite all their data stored in the removed groups.

{% endnote %}

The duration of the process depends mostly on the amount of data stored in the removed groups and on the number of tablets that have data in them: it can range from seconds for an empty database to hours for large ones.

## Limitations

* At least one storage unit of each kind must remain in the database. A request that removes all storage units of a kind is rejected.
* Storage units can only be removed from dedicated databases. Shared and serverless databases don't support this operation.
* You can't choose which groups are removed: Hive selects them automatically, preferring the groups with the least amount of data.
* The remaining groups of the pool must have enough free space and throughput to accommodate the data moved from the removed groups.

## Removing storage units

Storage units are removed with the `AlterDatabase` call of the CMS gRPC service `Ydb.Cms.V1.CmsService`. The request and response formats are described in the [ydb_cms.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/public/api/protos/ydb_cms.proto) file. You can make the call with any gRPC client. The examples below use [grpcurl](https://github.com/fullstorydev/grpcurl) together with the {{ ydb-short-name }} API protos from the [{{ ydb-short-name }} repository](https://github.com/ydb-platform/ydb).

The user making the requests must be a cluster administrator.

### Getting an authentication token

```bash
{{ ydb-cli }} -e grpcs://<cluster-endpoint>:2135 -d /Root --user <user> auth get-token -f > ~/ydb_token
```

### Checking the current storage units

Get the database status with the `GetDatabaseStatus` call:

```bash
grpcurl -cacert <path-to-ca-cert> \
  -H "x-ydb-auth-ticket: $(cat ~/ydb_token)" \
  -import-path <path-to-ydb-repo> \
  -proto ydb/public/api/grpc/ydb_cms_v1.proto \
  -d '{"path": "/Root/db1"}' \
  <cluster-endpoint>:2135 \
  Ydb.Cms.V1.CmsService/GetDatabaseStatus
```

The result contains, among other fields:

* `state` — the database state;
* `requiredResources.storageUnits` — the number of storage units of each kind that the database should have;
* `allocatedResources.storageUnits` — the number of storage units of each kind that are actually allocated to the database now.

Result example (only the relevant fields are shown):

```json
{
  "path": "/Root/db1",
  "state": "RUNNING",
  "requiredResources": {
    "storageUnits": [
      { "unitKind": "ssd", "count": "8" }
    ]
  },
  "allocatedResources": {
    "storageUnits": [
      { "unitKind": "ssd", "count": "8" }
    ]
  },
  "generation": "3"
}
```

{% note info %}

The CMS API returns results wrapped into an operation: the `GetDatabaseStatusResult` message is packed into the `operation.result` field of the response. The examples in this article show only the contents of `operation.result`.

{% endnote %}

### Starting the removal

Call `AlterDatabase` and list the storage units to remove in the `storage_units_to_remove` field. The example below removes 3 storage units of the `ssd` kind from the `/Root/db1` database:

```bash
grpcurl -cacert <path-to-ca-cert> \
  -H "x-ydb-auth-ticket: $(cat ~/ydb_token)" \
  -import-path <path-to-ydb-repo> \
  -proto ydb/public/api/grpc/ydb_cms_v1.proto \
  -d '{
        "path": "/Root/db1",
        "storage_units_to_remove": [
          { "unit_kind": "ssd", "count": 3 }
        ]
      }' \
  <cluster-endpoint>:2135 \
  Ydb.Cms.V1.CmsService/AlterDatabase
```

Request parameters:

* `path` — the full path to the database;
* `storage_units_to_remove` — the list of storage units to remove:
  * `unit_kind` — the kind of storage units, which is the same as the kind of the database's storage pool (for example, `ssd` for the `/Root/db1:ssd` pool);
  * `count` — the number of storage units (storage groups) to remove;
* `generation` — optional. If set, the request is applied only if the current database generation, returned by `GetDatabaseStatus`, matches this value. Use it to make sure that nobody changed the database since you checked its status;
* `idempotency_key` — optional. If set, a repeated request with the same key is not applied again, which makes it safe to retry the request.

The request is completed as soon as the new number of storage units is saved. The operation status `SUCCESS` means that the removal has started, not that it has finished.

If the request is rejected, the response contains the `BAD_REQUEST` or `UNSUPPORTED` status and a description of the error. For example, `Not enough units of kind 'ssd' to remove` means that the request would remove all storage units of that kind.

### Tracking the progress

Call `GetDatabaseStatus` periodically. While the groups are being removed:

* `state` is `REMOVING_STORAGE_UNITS`;
* `requiredResources.storageUnits` already shows the new, smaller number of storage units;
* `allocatedResources.storageUnits` still shows the old number of storage units.

The removal is complete when `state` returns to `RUNNING` and the number of allocated storage units matches the number of required storage units:

```json
{
  "path": "/Root/db1",
  "state": "RUNNING",
  "requiredResources": {
    "storageUnits": [
      { "unitKind": "ssd", "count": "5" }
    ]
  },
  "allocatedResources": {
    "storageUnits": [
      { "unitKind": "ssd", "count": "5" }
    ]
  },
  "generation": "4"
}
```

If something prevents the removal from progressing, the problems are reported in the `issues` field of the `GetDatabaseStatus` result.

You can also check the number of groups in the storage pool (the `Groups_TOTAL` column) with [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

```bash
ydb-dstool -e <bs_endpoint> pool list
```

### Changing or canceling the removal

The database can still be altered while the groups are being removed. To remove fewer groups than originally requested, or to cancel the removal, add the storage units back with the `storage_units_to_add` field of `AlterDatabase`:

```bash
grpcurl -cacert <path-to-ca-cert> \
  -H "x-ydb-auth-ticket: $(cat ~/ydb_token)" \
  -import-path <path-to-ydb-repo> \
  -proto ydb/public/api/grpc/ydb_cms_v1.proto \
  -d '{
        "path": "/Root/db1",
        "storage_units_to_add": [
          { "unit_kind": "ssd", "count": 3 }
        ]
      }' \
  <cluster-endpoint>:2135 \
  Ydb.Cms.V1.CmsService/AlterDatabase
```

Groups that haven't been deleted yet become active again and continue to serve the database. Data that has already been moved to other groups stays there.

Similarly, you can remove more storage units while a previous removal is still in progress: the new request extends the set of groups being removed.
