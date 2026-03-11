# admin cluster bridge failover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

The `admin cluster bridge failover` command performs an [emergency failover](../../../../concepts/bridge.md#failover) for a pile when it is unavailable. If needed, you can specify which pile should become the new `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge failover [options...]
```

* `global options` — CLI [global options](../global-options.md).
* `options` — [subcommand options](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge failover --help
```

## Subcommand options {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the unavailable pile. ||
|| `--new-primary <pile>` | Name of the pile that should become the new `PRIMARY` pile. Specify it if the unavailable pile was `PRIMARY`. ||
|#

## Requirements {#requirements}

- If the current `PRIMARY` is unavailable, you must specify `--new-primary` and choose a pile in the `SYNCHRONIZED` state. If `--new-primary` is omitted, or a pile in a state other than `SYNCHRONIZED` is selected, the command will return an error without making any changes.
- The cluster will not transition into an invalid state: if requirements are violated, the command does not change anything and reports an error.
- If a pile is not down, but you need to take it out, use a [planned takedown](../../../../concepts/bridge.md#takedown) via the [`takedown`](takedown.md) command.

## Examples {#examples}

Perform an emergency failover for an unavailable pile named `pile-a`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a
```

Perform an emergency failover for an unavailable `PRIMARY` pile and set a synchronized pile as the new `PRIMARY`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a --new-primary pile-b
```

### Verify the result {#verify}

Using the [list](list.md) command, verify that the unavailable pile has moved to the `DISCONNECTED` state and (if `--new-primary` was specified) a new `PRIMARY` pile was selected:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
