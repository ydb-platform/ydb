# Configuring Time to Live (TTL)

This section contains recipes for configuration of table's TTL with YQL.

## Enabling TTL for an existing table {#enable-on-existent-table}

In the example below, the items of the `mytable` table will be deleted an hour after the time set in the `created_at` column:

```yql
ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON created_at);
```

{% note tip %}

An `Interval` is created from a string literal in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format with [some restrictions](../builtins/basic#data-type-literals).

{% endnote %}

The example below shows how to use the `modified_at` column with a numeric type (`Uint32`) as a TTL column. The column value is interpreted as the number of seconds since the Unix epoch:

```yql
ALTER TABLE `mytable` SET (TTL = Interval("PT1H") ON modified_at AS SECONDS);
```


## Enabling TTL for a newly created table {#enable-for-new-table}

For a newly created table, you can pass TTL settings along with the table description:

```yql
CREATE TABLE `mytable` (
  id Uint64,
  expire_at Timestamp,
  PRIMARY KEY (id)
) WITH (
  TTL = Interval("PT1H") ON expire_at
);
```

## Disabling TTL {#disable}

```yql
ALTER TABLE `mytable` RESET (TTL);
```

