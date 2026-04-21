# admin cluster bridge switchover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge switchover` command to perform a smooth, planned transition of the specified pile to `PRIMARY` via the intermediate `PROMOTED` state. For details, see the [scenario description](../../../../concepts/bridge.md#switchover).

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge switchover [options...]
```

* `global options` — [global parameters](../global-options.md) for the CLI.
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

Transition pile `pile-b` from `SYNCHRONIZED` to `PRIMARY` via the intermediate `PROMOTED` state:

```bash
{{ ydb-cli }} admin cluster bridge switchover --new-primary pile-b
```

### Verifying the result {#verify}

After a short time (a few minutes), verify that pile states have changed correctly using the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
