# device list

Use the `device list` subcommand to get a list of storage devices available on the {{ ydb-short-name }} cluster.

General format of the command:

```bash
ydb-dstool [global options ...] device list [list options ...]
```

* `global options`: [Global options](global-options.md).
* `list options`: [Subcommand options](#options).

View a description of the command to get a list of devices:

```bash
ydb-dstool device list --help
```

## Subcommand options {#options}

| Option | Description |
---|---
| `-H`, `--human-readable` | Output data in human-readable format. |
| `--sort-by` | Sort column.<br/>Use one of the values: `SerialNumber`, `FQDN`, `Path`, `Type`, `StorageStatus`, or `NodeId:PDiskId`. |
| `--reverse` | Use a reverse sort order. |
| `--format` | Output format.<br/>Use one of the values: `pretty`, `json`, `tsv`, or `csv`. |
| `--no-header` | Do not output the row with column names. |
| `--columns` | List of columns to be output.<br/>Use one or more of the values: `SerialNumber`, `FQDN`, `Path`, `Type`, `StorageStatus`, or  `NodeId:PDiskId`. |
| `-A`, `--all-columns` | Output all columns. |

## Examples {#examples}

The following command will output a list of devices available in the cluster:

```bash
ydb-dstool -e node-5.example.com device list
```

Result:

```text
┌────────────────────┬────────────────────┬────────────────────────────────┬──────┬───────────────────────────┬────────────────┐
│ SerialNumber       │ FQDN               │ Path                           │ Type │ StorageStatus             │ NodeId:PDiskId │
├────────────────────┼────────────────────┼────────────────────────────────┼──────┼───────────────────────────┼────────────────┤
│ PHLN123301H41P2BGN │ node-1.example.com │ /dev/disk/by-partlabel/nvme_04 │ NVME │ FREE                      │ NULL           │
│ PHLN123301A62P2BGN │ node-6.example.com │ /dev/disk/by-partlabel/nvme_03 │ NVME │ PDISK_ADDED_BY_DEFINE_BOX │ [6:1001]       │
...
```
