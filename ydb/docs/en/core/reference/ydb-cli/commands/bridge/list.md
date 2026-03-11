# admin cluster bridge list

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

The `admin cluster bridge list` command prints the status of each pile in bridge mode.

Command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge list [options...]
```

* `global options` — CLI [global options](../global-options.md).
* `options` — [subcommand options](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge list --help
```

## Subcommand options {#options}

#|
|| Name | Description ||
|| `--format <pretty, json, csv>` | Output format. Valid values: `pretty`, `json`, `csv`. Default: `pretty`. ||
|#

## Examples {#examples}

Show the pile list in a human-readable format:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: PRIMARY
pile-b: SYNCHRONIZED
```

Show the status in JSON format:

```bash
{{ ydb-cli }} admin cluster bridge list --format json

{
  "pile-a": "PRIMARY",
  "pile-b": "SYNCHRONIZED"
}
```

Show the status in CSV format:

```bash
{{ ydb-cli }} admin cluster bridge list --format csv

pile,state
pile-a,PRIMARY
pile-b,SYNCHRONIZED
```
