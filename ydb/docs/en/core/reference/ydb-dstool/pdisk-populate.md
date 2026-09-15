# pdisk populate

Use the `pdisk populate` subcommand to move a selected set of [VDisks](../../concepts/glossary.md#vdisk) to one [PDisk](../../concepts/glossary.md#pdisk). The command can save the active VDisks of a source PDisk to a snapshot file and then use that file to populate a destination PDisk.

{% note warning %}

Use `pdisk populate` for controlled device testing: it lets you place exactly the same set of VDisks on a new device and compare how the old and new devices handle that workload.

{% endnote %}

The [Blob Storage Controller](../../concepts/glossary.md#ds-controller) validates all selected VDisks and schedules their reassignment in a single atomic configuration transaction. If any VDisk cannot be reassigned, the configuration is not changed. The data migration itself continues asynchronously after the transaction is applied.

General format of the command:

```bash
ydb-dstool [global options ...] pdisk populate [populate options ...]
```

* `global options`: [Global options](global-options.md).
* `populate options`: [Subcommand options](#options).

View a description of the command:

```bash
ydb-dstool pdisk populate --help
```

## Subcommand options {#options}

| Option | Description |
| --- | --- |
| `--snapshot-from-pdisk <NodeId:PDiskId>` | Snapshot mode. Collect active VDisks from the specified PDisk. Donor VDisks are skipped. |
| `-d`, `--destination-pdisk <NodeId:PDiskId>` | Populate mode. Move the VDisks listed in `--snapshot-file` to the specified PDisk. |
| `--snapshot-file <PATH>` | In snapshot mode, write the VDisk list as JSON to the specified file. In populate mode, read the VDisk list from this file. This option is required in populate mode. |
| `--suppress-donor-mode` | Do not leave the previous VDisk locations in donor mode after reassignment. This option is only available in populate mode. |
| `--format <FORMAT>` | Output format: `pretty` (default) or `json`. |

Exactly one of `--snapshot-from-pdisk` and `--destination-pdisk` must be specified.

## Example {#example}

First, save the list of active VDisks on PDisk `[1:1000]` to a snapshot file:

```bash
ydb-dstool -e node-1.example.com pdisk populate \
  --snapshot-from-pdisk '[1:1000]' \
  --snapshot-file /tmp/pdisk-1-1000.json
```

The snapshot file has the following format:

```json
{
  "pdisk_id": "[1:1000]",
  "vdisk_ids": [
    "[80000001:_:0:0:0]",
    "[80000002:_:0:1:0]"
  ]
}
```

Review the snapshot and then move the listed VDisks to PDisk `[2:1000]`:

```bash
ydb-dstool -e node-1.example.com pdisk populate \
  --destination-pdisk '[2:1000]' \
  --snapshot-file /tmp/pdisk-1-1000.json
```

The command skips VDisks with `GroupId=0`, because they cannot be migrated. If the snapshot contains only such VDisks, the command returns an error without changing the configuration.
