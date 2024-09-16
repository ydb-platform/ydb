# Modifying additional table parameters

Most parameters of row and column tables in {{ ydb-short-name }}, listed on the [table description]({{ concept_table }}) page, can be modified using the `ALTER` command.

Generally, the command to modify any table parameter looks as follows:

```sql
ALTER TABLE table_name SET (key = value);
```

`key` — the name of the parameter, `value` — its new value.

Example of modifying the `TTL` parameter, which controls the time-to-live of records in a table:

```sql
ALTER TABLE series SET (TTL = Interval("PT0S") ON expire_at);
```

## Resetting Additional Table Parameters {#additional-reset}

Some table parameters in {{ ydb-short-name }}, listed on the [table description]({{ concept_table }}) page, can be reset using the `ALTER` command. The command to reset a table parameter looks as follows:

```sql
ALTER TABLE table_name RESET (key);
```

`key` — the name of the parameter.

For example, such a command will reset (remove) the `TTL` settings for row or column tables:

```sql
ALTER TABLE series RESET (TTL);
```