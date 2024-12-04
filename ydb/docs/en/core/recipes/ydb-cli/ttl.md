# Configuring Time to Live (TTL)

This section contains recipes for configuration of table's TTL with {{ ydb-short-name }} CLI.

## Enabling TTL for an existing table {#enable-on-existent-table}

In the example below, the items of the `mytable` table will be deleted an hour after the time set in the `created_at` column:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column created_at --expire-after 3600 mytable
```

The example below shows how to use the `modified_at` column with a numeric type (`Uint32`) as a TTL column. The column value is interpreted as the number of seconds since the Unix epoch:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl set --column modified_at --expire-after 3600 --unit seconds mytable
```

## Disabling TTL {#disable}

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table ttl reset mytable
```

## Getting TTL settings {#describe}

The current TTL settings can be obtained from the table description:

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> scheme describe mytable
```

