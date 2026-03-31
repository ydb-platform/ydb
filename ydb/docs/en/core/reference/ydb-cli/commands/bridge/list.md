# admin cluster bridge list

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge list` command to list the state of each pile in [bridge mode](../../../../concepts/bridge.md).

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge list [options...]
```

* `global options` — [global parameters](../global-options.md) for the CLI.
* `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge list --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--format <pretty, json, csv>` | Output format. Valid values: `pretty`, `json`, `csv`. Default: `pretty`. ||
|#

## Examples {#examples}

List piles in human-readable format:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: PRIMARY
pile-b: SYNCHRONIZED
```

Output state in JSON format:

```bash
{{ ydb-cli }} admin cluster bridge list --format json

{
  "pile-a": "PRIMARY",
  "pile-b": "SYNCHRONIZED"
}
```

Output state in CSV format:

```bash
{{ ydb-cli }} admin cluster bridge list --format csv

pile,state
pile-a,PRIMARY
pile-b,SYNCHRONIZED
```
