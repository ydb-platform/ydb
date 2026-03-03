# admin cluster bridge switchover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge switchover` command to perform a smooth, planned switch of the specified pile to the `PRIMARY` state via the intermediate `PROMOTED` state. For more information, see the [scenario description](../../../../concepts/bridge.md#switchover).

{% include [danger-warning](../configuration/_includes/danger-warning.md) %}

Command format:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge switchover [options...]
```

* `global options` — CLI [global parameters](../global-options.md).
* `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge switchover --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--new-primary <pile>` | Name of the pile that should become the new PRIMARY. ||
|#

## Requirements {#requirements}

- The target pile must be in the `SYNCHRONIZED` state.

## Examples {#examples}

Switching pile `pile-b` from `SYNCHRONIZED` to `PRIMARY` via the intermediate `PROMOTED` state:

```bash
{{ ydb-cli }} admin cluster bridge switchover --new-primary pile-b
```

## Verifying the result {#verify}

Verify that pile states have changed correctly after some time (a few minutes) using the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
