# admin cluster bridge takedown

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

The `admin cluster bridge takedown` command performs a [planned takedown](../../../../concepts/bridge.md#takedown) of a pile. If the current `PRIMARY` is being taken down, you must specify a new `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge takedown [options...]
```

* `global options` — CLI [global options](../global-options.md).
* `options` — [subcommand options](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge takedown --help
```

## Subcommand options {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the pile to stop gracefully. ||
|| `--new-primary <pile>` | Name of the pile that should become the new `PRIMARY` if the current `PRIMARY` is being taken down. ||
|#

## Requirements {#requirements}

- If the current `PRIMARY` is being taken down, you must specify `--new-primary` and choose a pile in the `SYNCHRONIZED` state.

## Examples {#examples}

Take a `SYNCHRONIZED` pile `pile-b` out of the cluster:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile pile-b
```

Take the `PRIMARY` pile `pile-a` out of the cluster and switch pile `pile-b` from `SYNCHRONIZED` to `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile pile-a --new-primary pile-b
```

## Verify the result {#verify}

Check final pile states using the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: PRIMARY
pile-b: DISCONNECTED
```

If the current `PRIMARY` was taken down with `--new-primary`, make sure the selected pile became `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
