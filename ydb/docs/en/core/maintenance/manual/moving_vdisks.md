# Moving VDisks

Sometimes you may need to free up a block store volume to replace equipment. Or a VDisk may be in active use, affecting the performance of other VDisks running on the same PDisk. In cases like this, VDisks need to be moved.

## Move one VDisk from a block store volume {#moving_vdisk}

Get the VDisk ID using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

```bash
ydb-dstool -e <bs_endpoint> vdisk list --format tsv --columns VDiskId --no-header
```

Move the selected VDisk:

```bash
ydb-dstool -e <bs_endpoint> vdisk evict --vdisk-ids VDISK_ID
```

The Blob Storage Controller selects a suitable destination PDisk according to the cluster placement rules. `VDISK_ID` is a VDisk ID in the `[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx]` format.

## Move all VDisks for planned maintenance {#moving_pdisk}

### Automatic relocation during planned maintenance

For routine hardware maintenance, set the source PDisk maintenance status to `LONG_TERM_MAINTENANCE_PLANNED`:

```bash
ydb-dstool -e <bs_endpoint> pdisk set \
  --maintenance-status LONG_TERM_MAINTENANCE_PLANNED \
  --pdisk-ids "[NodeId:PDiskId]"
```

This status prevents new VDisks from being placed on the PDisk and instructs SelfHeal to move its existing VDisks asynchronously. The Blob Storage Controller selects suitable destination PDisks according to the cluster placement rules.

If the PDisk remains in the cluster after maintenance and can accept new VDisks again, clear the maintenance request:

```bash
ydb-dstool -e <bs_endpoint> pdisk set \
  --maintenance-status NO_REQUEST \
  --pdisk-ids "[NodeId:PDiskId]"
```

### Controlled manual relocation

If you need to control which VDisks are moved, prevent new placements on the source PDisk and evict the selected VDisks manually:

1. Set the source PDisk maintenance status to `NO_NEW_VDISKS`:

   ```bash
   ydb-dstool -e <bs_endpoint> pdisk set \
     --maintenance-status NO_NEW_VDISKS \
     --pdisk-ids "[NodeId:PDiskId]"
   ```

1. Get the IDs of the VDisks located on the source PDisk:

   ```bash
   ydb-dstool -e <bs_endpoint> vdisk list \
     --format tsv --columns VDiskId NodeId:PDiskId --no-header \
     | fgrep '[NodeId:PDiskId]'
   ```

1. Evict the selected VDisks:

   ```bash
   ydb-dstool -e <bs_endpoint> vdisk evict --vdisk-ids VDISK_ID1 ... VDISK_IDN
   ```

* `VDISK_ID1 ... VDISK_IDN`: VDisk IDs in the `[GroupId:GroupGeneration:FailRealmIdx:FailDomainIdx:VDiskIdx]` format, separated by spaces.
* `NodeId:PDiskId`: ID of the source PDisk.

If the PDisk can accept new VDisks after the operation, set its maintenance status back to `NO_REQUEST`.

## Reproduce a PDisk workload on another device {#testing_device}

Use [`pdisk populate`](../../reference/ydb-dstool/pdisk-populate.md) only for controlled device testing, when you need to move exactly the same set of VDisks to a new device and compare its performance with the old device under the same workload.

First, save the active VDisks of the old PDisk to a snapshot:

```bash
ydb-dstool -e <bs_endpoint> pdisk populate \
  --snapshot-from-pdisk '[SourceNodeId:SourcePDiskId]' \
  --snapshot-file /tmp/source-pdisk.json
```

Review the snapshot, then populate the new PDisk with the same VDisks:

```bash
ydb-dstool -e <bs_endpoint> pdisk populate \
  --destination-pdisk '[DestinationNodeId:DestinationPDiskId]' \
  --snapshot-file /tmp/source-pdisk.json
```

Unlike `vdisk evict`, this command places all selected VDisks on the explicitly specified destination PDisk.

## Move VDisks from a broken/missing block store volume {#removal_from_a_broken_device}

If SelfHeal is disabled or fails to move VDisks automatically, you'll have to run this operation manually:

1. Go to [monitoring](../../reference/ydb-ui/ydb-monitoring.md) and make sure that the VDisk has actually failed.
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

1. Go to [monitoring](../../reference/ydb-ui/ydb-monitoring.md) and make sure that the VDisk is actually operable.
1. Get the appropriate `[NodeId:PDiskId]` using [{{ ydb-short-name }} DSTool](../../reference/ydb-dstool/index.md):

   ```bash
   ydb-dstool -e <bs_endpoint> pdisk list
   ```

1. Enable the PDisk back:

   ```bash
   ydb-dstool -e <bs_endpoint> pdisk set --status ACTIVE --pdisk-ids "[NodeId:PDiskId]"
   ```
