# Moving VDisks

Sometimes you may need to free up a block store volume to replace equipment. Or a VDisk may be in active use, affecting the performance of other VDisks running on the same PDisk. In cases like this, VDisks need to be moved.

## Move a VDisk from a block store volume {#moving_vdisk}

Get a list of VDisk IDs using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

```bash
ydb-dstool -e <bs_endpoint> vdisk list --format tsv --columns VDiskId --no-header
```

To move a VDisk from a block store volume, run the following commands on the cluster node:

```bash
ydb-dstool -e <bs_endpoint> vdisk evict --vdisk-ids VDISK_ID1 ... VDISK_IDN
ydbd admin bs config invoke --proto 'Command { ReassignGroupDisk { GroupId: <Storage group ID> GroupGeneration: <Storage group generation> FailRealmIdx: <FailRealm> FailDomainIdx: <FailDomain> VDiskIdx: <Slot number> } }'
```

* `VDISK_ID1 ... VDISK_IDN`: The list of VDisk IDs like `[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx]`. The IDs are separated by a space.
* `GroupId`: The ID of the storage group.
* `GroupGeneration`: Storage group generation.
* `FailRealmIdx`: Fail realm number.
* `FailDomainIdx`: Fail domain number.
* `VDiskIdx`: Slot number.

## Move VDisks from a broken/missing block store volume {#removal_from_a_broken_device}

If SelfHeal is disabled or fails to move VDisks automatically, you'll have to run this operation manually:

1. Go to [monitoring](../../reference/embedded-ui/ydb-monitoring.md) and make sure that the VDisk has actually failed.
1. Get the appropriate `[NodeId:PDiskId]` using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

   ```bash
   ydb-dstool -e <bs_endpoint> vdisk list | fgrep VDISK_ID
   ```

1. Move the VDisk:

   ```bash
   ydb-dstool -e <bs_endpoint> pdisk set --status BROKEN --pdisk-ids "[NodeId:PDiskId]"
   ```

## Enable the VDisk back after reassignment {#return_a_device_to_work}

To enable the VDisk back after reassignment:

1. Go to [monitoring](../../reference/embedded-ui/ydb-monitoring.md) and make sure that the VDisk is actually operable.
1. Get the appropriate `[NodeId:PDiskId]` using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

   ```bash
   ydb-dstool -e <bs_endpoint> pdisk list
   ```

1. Enable the PDisk back:

   ```bash
   ydb-dstool -e <bs_endpoint> pdisk set --status ACTIVE --pdisk-ids "[NodeId:PDiskId]"
   ```
