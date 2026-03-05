# admin cluster bridge rejoin

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge rejoin` command to [return](../../../../concepts/bridge.md#rejoin) the specified pile to the cluster after maintenance or recovery. After running the command, the pile is expected to transition from the `DISCONNECTED` state to `NOT_SYNCHRONIZED`, followed by automatic synchronization and transition to the `SYNCHRONIZED` state.

{% include [danger-warning](../configuration/_includes/danger-warning.md) %}

Command format:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge rejoin [options...]
```

* `global options` — global parameters.
* `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the pile to return to the cluster. ||
|#

## Requirements {#requirements}

- The pile must be in the `DISCONNECTED` state before returning.

## Examples {#examples}

Returning pile `pile-a` from the `DISCONNECTED` state:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile pile-a
```

## Verifying the result {#verify}

Immediately after running the command, the pile is expected to transition to the `NOT_SYNCHRONIZED` state. Verify the result using the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: NOT_SYNCHRONIZED
pile-b: PRIMARY
```

After synchronization completes, the pile transitions to the `SYNCHRONIZED` state:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
