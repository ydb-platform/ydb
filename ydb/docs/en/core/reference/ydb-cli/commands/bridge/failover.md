# admin cluster bridge failover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge failover` command to perform [emergency disconnection](../../../../concepts/bridge.md#failover) of a pile when it is unavailable. You can optionally specify the pile that will become the new `PRIMARY`.

{% include [danger-warning](../configuration/_includes/danger-warning.md) %}

Command format:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge failover [options...]
```

* `global options` — CLI [global parameters](../global-options.md).
* `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge failover --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the unavailable pile. ||
|| `--new-primary <pile>` | Name of the pile that should become the new `PRIMARY` pile. Specify if the unavailable pile was `PRIMARY`. ||
|#

## Requirements {#requirements}

- If the current `PRIMARY` is unavailable, you must specify `--new-primary` and select a pile in the `SYNCHRONIZED` state. If `--new-primary` is omitted or a pile in a state other than `SYNCHRONIZED` is selected, the command will return an error without any changes.
- The cluster will not transition to an invalid state: if requirements are violated, the command changes nothing and reports an error.
- If the pile has not failed but needs to be disconnected, use [planned disconnection](../../../../concepts/bridge.md#takedown) — the [`takedown`](takedown.md) command.

## Examples {#examples}

Performing emergency disconnection for an unavailable pile named `pile-a`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a
```

Performing emergency disconnection for an unavailable `PRIMARY` pile and designating a synchronized pile as the new `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a --new-primary pile-b
```

### Verifying the result {#verify}

Use the [list](list.md) command to verify that the unavailable pile has been transitioned to the `DISCONNECTED` state and (if `--new-primary` was specified) a new `PRIMARY` pile has been selected:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
