# Deleting TTL parameters

Use the `table ttl drop` subcommand to delete [TTL](../../concepts/ttl.md) for the specified table.

General format of the command:

```bash
{{ ydb-cli }} [global options...] table ttl drop [options...] <table path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `table path`: The table path.

View a description of the TTL delete command:

```bash
{{ ydb-cli }} table ttl drop --help
```

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Delete TTL for the `series` table:

```bash
{{ ydb-cli }} -p db1 table ttl drop \
  series
```