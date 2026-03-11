# admin cluster bridge switchover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

The `admin cluster bridge switchover` command performs a smooth, planned switch of the specified pile to the `PRIMARY` state via an intermediate `PROMOTED` state. For details, see the [scenario description](../../../../concepts/bridge.md#switchover).

{% include [danger-warning](../_includes/danger-warning.md) %}

Command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge switchover [options...]
```

* `global options` — CLI [global options](../global-options.md).
* `options` — [subcommand options](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge switchover --help
```

## Subcommand options {#options}

#|
|| Name | Description ||
|| `--new-primary <pile>` | Name of the pile that should become the new PRIMARY. ||
|#

## Requirements {#requirements}

- The target pile must be in the `SYNCHRONIZED` state.

## Examples {#examples}

Switch pile `pile-b` from `SYNCHRONIZED` to `PRIMARY` via the intermediate `PROMOTED` state:

```bash
{{ ydb-cli }} admin cluster bridge switchover --new-primary pile-b
```

## Verify the result {#verify}

After some time (a few minutes), verify that pile states have changed correctly using the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
