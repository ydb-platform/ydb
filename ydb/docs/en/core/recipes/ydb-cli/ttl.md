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

## Enabling data eviction to S3-compatible external storage

{% include [OLTP_not_allow_note](../../_includes/not_allow_for_oltp_note.md) %}

To enable data eviction, an [external data source](../../concepts/datamodel/external_data_source.md) object that describes a connection to the external storage is needed. Refer to [YQL recipe](../../yql/reference/recipes/ttl.md#enable-tiering-on-existing-tables) for examples of creating an external data source.

The example below shows how to enable data eviction by executing a YQL-query from {{ ydb-short-name }} CLI. Rows of the table `mytable` will be moved to the bucket described in the external data source `/Root/s3_cold_data` one hour after the time recorded in the column `created_at` and will be deleted after 24 hours.

```bash
$ {{ ydb-cli }} -e <endpoint> -d <database> table query execute -q '
    ALTER TABLE `mytable` SET (
        TTL =
            Interval("PT1H") TO EXTERNAL DATA SOURCE `/Root/s3_cold_data`,
            Interval("PT24H") DELETE
        ON modified_at AS SECONDS
    );
'
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

