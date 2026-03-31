# admin cluster bridge failover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge failover` command to perform [emergency disable](../../../../concepts/bridge.md#failover) of a pile when it is unavailable. You can optionally specify the pile that will become the new `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge failover [options...]
```

* `global options` — [global parameters](../global-options.md) for the CLI.
* `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge failover --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the unavailable pile. ||
|| `--new-primary <pile>` | Name of the pile that should become the new `PRIMARY`. Specify if the unavailable pile was `PRIMARY`. ||
|#

## Requirements {#requirements}

- If the current `PRIMARY` is unavailable, you must specify `--new-primary` and choose a pile in the `SYNCHRONIZED` state. If `--new-primary` is missing or the chosen pile is not in `SYNCHRONIZED` state, the command returns an error without making any changes.
- The cluster will not enter an invalid state: if requirements are violated, the command makes no changes and reports an error.
- If the pile has not failed but you need to disable it, use [planned disable](../../../../concepts/bridge.md#takedown) — the [`takedown`](takedown.md) command.

## Examples {#examples}

Perform emergency disable for an unavailable pile named `pile-a`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a
```

Perform emergency disable for an unavailable `PRIMARY` pile and designate a synchronized pile as the new `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a --new-primary pile-b
```

### Verifying the result {#verify}

Use the [list](list.md) command to verify that the unavailable pile has been moved to `DISCONNECTED` and (if `--new-primary` was specified) a new `PRIMARY` has been selected:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
