# Secondary indexes

This section describes how to use [secondary indexes](../../concepts/secondary_indexes.md) for selecting data.

In general, transactions using a global index are [distributed transactions](../../concepts/transactions.md#distributed-tx). A query can be executed as a single-shard transaction in the following cases:

* Point read by primary key.
* Point read by index column if requested data is a primary key, part of it, or there is a copy of the data in a [covering index](../../concepts/secondary_indexes.md#covering)).
* Point blind write in a table with an [asynchronous index](../../concepts/secondary_indexes.md#async).

{% note warning %}

The size of a response to a client may not exceed 50 MB. The size of data extracted from a single table shard per YQL query may not exceed 5 GB. For large tables and queries, these limits may make it impossible to fully scan all table rows.

{% endnote %}

## Creating a table {#create}

To create a table named `series`, run the query:

```sql
CREATE TABLE series
(
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    views Uint64,
    PRIMARY KEY (series_id),
    INDEX views_index GLOBAL ON (views)
);
```

The `series` table has a key column named `series_id`. An index by primary key is created in {{ ydb-short-name }} automatically. Since the stored data is sorted in order of ascending primary key values, selecting data by this key will be efficient. An example of this selection is a search for all broadcasts of a series by its `series_id` from the `series` table. We also create an index named `views_index` to the `views` column. It will let you efficiently execute queries using it in the predicate.

For a more complex select, you can create multiple secondary indexes. For example, to create a table named `series` with two indexes, run the query:

```sql
CREATE TABLE series
(
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    views Uint64,
    PRIMARY KEY (series_id),
    INDEX views_index GLOBAL ON (views) COVER (release_date),
    INDEX date_index GLOBAL ON (release_date)
);
```

As a result, two secondary indexes are created: `views_index` to the `views` column and `date_index` to the `release_date` column. In this case, `views_index` contains a copy of the data from the `release_date` column.

{% note info %}

You can add a secondary index to an existing table without stopping the service. For more information about online index creation, see the [instructions](../../concepts/secondary_indexes.md#index-add).

{% endnote %}

## Inserting data {#insert}

Sample YQL queries use [prepared queries](https://en.wikipedia.org/wiki/Prepared_statement). To run them in the YQL editor, define the values of the parameters declared using the `DECLARE` statement.

To add data to the `series` table, run the query:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $title AS Utf8;
DECLARE $seriesInfo AS Utf8;
DECLARE $releaseDate AS Uint32;
DECLARE $views AS Uint64;

INSERT INTO series (series_id, title, series_info, release_date, views)
VALUES ($seriesId, $title, $seriesInfo, $releaseDate, $views);
```

## Updating data {#upsert}

You can save a new number of views for a particular series in the database with the `UPSERT` operation.

To update data in the `series` table, run the query:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $newViews AS Uint64;

UPSERT INTO series (series_id, views)
VALUES ($seriesId, $newViews);
```

## Data selection {#select}

Without using secondary indexes, a query to select records that match a certain predicate by number of views won't work effectively since {{ ydb-short-name }} scans all the rows of the `series` table to execute it. The query must explicitly specify which index to use.

To select the `series` table rows that match the predicate by number of views, run the query:

```sql
SELECT series_id, title, series_info, release_date, views
FROM series view views_index
WHERE views >= someValue
```

## Updating data using a secondary index {#update}

`UPDATE` statements don't let you indicate that a secondary index should be used to search for data, so an attempt to run `UPDATE ... WHERE indexed_field = $value` will result in a full scan of the table. To avoid this, you can first run `SELECT` by index to get the primary key value and then `UPDATE` by the primary key. You can also use `UPDATE ON`.

To update data in the `series` table, run the query:

```sql
$to_update = (
    SELECT pk_field, field1 = $f1, field2 = $f2, ...
    FROM   table1 view idx_field3
    WHERE  field3 = $f3)

UPDATE table1 ON SELECT * FROM $to_update
```

## Deleting data using a secondary index {#delete}

To delete data by secondary index, use `SELECT` with a predicate by secondary index and then call `DELETE ON`.

To delete all data about series with zero views from the `series` table, run the query:

```sql
DELETE FROM series ON
SELECT series_id, 
FROM series view views_index
WHERE views == 0;
```

