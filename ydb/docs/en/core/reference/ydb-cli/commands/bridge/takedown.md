# admin cluster bridge takedown

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge takedown` command to perform [planned disable](../../../../concepts/bridge.md#takedown) of a pile. If you are disabling the current `PRIMARY`, you must specify the new `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

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
|| `--pile <pile>` | Name of the pile to take out of the cluster. ||
|| `--new-primary <pile>` | Name of the pile that should become the new `PRIMARY` if the current `PRIMARY` is being disabled. ||
|#

## Requirements {#requirements}

- If you are disabling the current `PRIMARY`, you must specify `--new-primary` and choose a pile in the `SYNCHRONIZED` state.

## Examples {#examples}

Take `SYNCHRONIZED` pile `pile-b` out of the cluster:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile pile-b
```

Take `PRIMARY` pile `pile-a` out of the cluster and transition `pile-b` from `SYNCHRONIZED` to `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile pile-a --new-primary pile-b
```

### Verifying the result {#verify}

Verify the resulting pile states with the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: PRIMARY
pile-b: DISCONNECTED
```

If you disabled the current `PRIMARY` with `--new-primary`, verify that the chosen pile has become `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
