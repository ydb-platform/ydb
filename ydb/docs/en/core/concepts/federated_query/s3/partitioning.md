# Data partitioning in S3 ({{ objstorage-full-name }})

In S3 ({{ objstorage-full-name }}), it is possible to store very large volumes of data. At the same time, queries to this data may not need to touch all the data but only a part of it. If you describe the rules for marking the storage structure of your data in {{ydb-full-name}}, then data that is not needed for the query can even be skipped from being read from S3 ({{ objstorage-full-name }}). This mechanism significantly speeds up query execution without affecting the result.

For example, data is stored in the following directory structure:

```
year=2021
    month=01
    month=02
    month=03
year=2022
    month=01
```

The query below explicitly implies that only the data for February 2021 needs to be processed, and other data is not needed.

```sql
SELECT
    *
FROM
    objectstorage.'/'
WITH
(
    SCHEMA =
    (
        data String,
        year Int32,
        month Int32
    )
)
WHERE
    year=2021
    AND month=02
```

If the data partitioning scheme is not specified, then _all_ stored data will be read from S3 ({{ objstorage-full-name }}), but as a result of processing, data for all other dates will be discarded.

If you explicitly describe the storage structure, specifying that the data in S3 ({{ objstorage-full-name }}) is placed in directories by years and months

```sql
SELECT
    *
FROM
    objectstorage.'/'
WITH
(
    SCHEMA =
    (
        data String,
        year Int32,
        month Int32
    ),
    PARTITIONED_BY = "['year', 'month']"
)
WHERE
    year=2021
    AND month=02
```

then during the query execution, not all data will be read from S3 ({{ objstorage-full-name }}), but only the data for February 2021. This will significantly reduce the volume of data processed and speed up processing, while the results of both queries will be identical.

{% note info %}

The example above shows working with data at the level of [connections](../../datamodel/external_data_source.md). This example is chosen for illustrative purposes only. We strongly recommend using "data bindings" to work with data and not using direct work with connections.

{% endnote %}

## Syntax { #syntax }

When working at the connection level, partitioning is set using the `partitioned_by` parameter, where the list of columns is specified in JSON format.

```sql
SELECT
    *
FROM
    <connection>.<path>
WITH
(
    SCHEMA=(<field1>, <field2>, <field3>),
    PARTITIONED_BY="['field2', 'field3']"
)
```

In the `partitioned_by` parameter, the columns of the data schema by which the data stored in S3 ({{ objstorage-full-name }}) are partitioned are listed. The order of specifying fields in the `partitioned_by` parameter determines the nesting of S3 ({{ objstorage-full-name }}) directories within each other.

For example, `PARTITIONED_BY=['year', 'month']` defines the directory structure

```
year=2021
    month=01
    month=02
    month=03
year=2022
    month=01
```

And `partitioned_by=['month', 'year']` defines another directory structure

```
month=01
    year=2021
    year=2022
month=02
    year=2021
month=03
    year=2021
```

## Supported data types

Partitioning is possible only with the following set of YQL data types:
- Uint16, Uint32, Uint64
- Int16, Int32, Int64
- String, Utf8

When using other types for specifying partitioning, an error is returned.

## Supported storage path formats

The storage path format, where the name of each directory explicitly specifies the column name, is called the "[Hive-Metastore format](https://en.wikipedia.org/wiki/Apache_Hive)" or simply the "Hive format."

This format looks as follows:
```
month=01
    year=2021
    year=2022
month=02
    year=2021
month=03
    year=2021
```

{% note warning %}

The basic partitioning mode in {{ydb-full-name}} supports only the Hive format.

{% endnote %}

Use the [Extended Data Partitioning](partition_projection.md) mode to specify arbitrary storage paths.