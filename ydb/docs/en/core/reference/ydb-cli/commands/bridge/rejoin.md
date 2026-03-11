# admin cluster bridge rejoin

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

The `admin cluster bridge rejoin` command returns a pile to the cluster after maintenance or recovery. After running the command, the pile is expected to transition from `DISCONNECTED` to `NOT_SYNCHRONIZED`, then be synchronized automatically and transition to `SYNCHRONIZED`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge rejoin [options...]
```

* `global options` — CLI [global options](../global-options.md).
* `options` — [subcommand options](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --help
```

## Subcommand options {#options}

#|
|| Name | Description ||
|| `--pile <pile>` | Name of the pile to return to the cluster. ||
|#

## Requirements {#requirements}

- Before being returned, the pile must be in the `DISCONNECTED` state.

## Examples {#examples}

Return pile `pile-a` from the `DISCONNECTED` state:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile pile-a
```

## Verify the result {#verify}

Immediately after running the command, the pile is expected to transition to the `NOT_SYNCHRONIZED` state. Check the result using the [list](list.md) command:

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
