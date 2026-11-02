# admin cluster bridge list

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge list` command to list pile states, configuration generation, cluster conversion progress, and pending pile operations in [bridge mode](../../../../concepts/bridge.md). This command is read-only and does not ask for confirmation.

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge list [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge list --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--format <pretty, json, csv>` | Output format. Valid values: `pretty`, `json`, `csv`. Default: `pretty`. |

## Examples {#examples}

List piles in human-readable format:

```bash
{{ ydb-cli }} admin cluster bridge list
```

```text
Pile pile-a: PRIMARY
Pile pile-b: SYNCHRONIZED
Generation: 4
```

During conversion, the output also contains:

```text
Conversion from non-bridged cluster is in progress, piles can't be added yet
```

During pile addition or removal, it contains the pending operation and, after a failed attempt, its error:

```text
Pending operation on pile pile-b: ADD
Last attempt has failed (being retried): <error>
```

Where `<error>` is the BSC error returned by the last attempt. The operation is retried automatically.

Output state in JSON format:

```bash
{{ ydb-cli }} admin cluster bridge list --format json
```

```json
{
  "generation": 4,
  "piles": [
    {
      "pile_name": "pile-a",
      "state": "PRIMARY"
    },
    {
      "pile_name": "pile-b",
      "state": "SYNCHRONIZED"
    }
  ],
  "conversion_in_progress": false
}
```

If a pile operation is pending, the JSON output additionally contains `pending_pile_operation` with `pile_name`, `kind`, and `error_reason` fields. Use JSON output for automation.

Output state in CSV format:

```bash
{{ ydb-cli }} admin cluster bridge list --format csv
```

```csv
pile_name,state,generation
pile-a,PRIMARY,4
pile-b,SYNCHRONIZED,4
```
