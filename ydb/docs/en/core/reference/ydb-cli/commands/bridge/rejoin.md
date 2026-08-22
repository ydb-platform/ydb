# admin cluster bridge rejoin

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge rejoin` command to [return](../../../../concepts/bridge.md#rejoin) the specified pile to the cluster after maintenance or recovery. After the command runs, the pile is expected to transition from `DISCONNECTED` to `NOT_SYNCHRONIZED`, then sync automatically and transition to `SYNCHRONIZED`.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

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

- The pile must be in the `DISCONNECTED` state before it can be returned.

## Examples {#examples}

Return pile `pile-a` from `DISCONNECTED` state:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile pile-a
```

### Verifying the result {#verify}

Right after the command runs, the pile is expected to transition to `NOT_SYNCHRONIZED`. Verify with the [list](list.md) command:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: NOT_SYNCHRONIZED
pile-b: PRIMARY
```

After synchronization completes, the pile transitions to `SYNCHRONIZED`:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
