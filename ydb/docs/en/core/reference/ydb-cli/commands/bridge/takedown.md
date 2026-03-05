# admin cluster bridge takedown

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge takedown` command to perform [planned disconnection](../../../../concepts/bridge.md#takedown) of a pile. If disconnecting the current `PRIMARY`, you must specify the new `PRIMARY`.

{% include [danger-warning](../configuration/_includes/danger-warning.md) %}

Command format:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge takedown [options...]
```

* `global options` — global parameters.
* `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge takedown --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the pile to gracefully shut down. ||
|| `--new-primary <pile>` | Name of the pile that should become the new `PRIMARY` if disconnecting the current `PRIMARY`. ||
|#

## Requirements {#requirements}

- If disconnecting the current `PRIMARY`, you must specify `--new-primary` and select a pile in the `SYNCHRONIZED` state.

## Examples {#examples}

Taking `SYNCHRONIZED` pile `pile-b` out of the cluster:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile pile-b
```

Taking `PRIMARY` pile `pile-a` out of the cluster and switching pile `pile-b` from `SYNCHRONIZED` to `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile pile-a --new-primary pile-b
```

## Verifying the result {#verify}

Verify the final pile states using the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: PRIMARY
pile-b: DISCONNECTED
```

If disconnecting the current `PRIMARY` with `--new-primary` specified, verify that the selected pile has become `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
