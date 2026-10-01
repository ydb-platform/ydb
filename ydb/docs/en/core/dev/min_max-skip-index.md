# min_max index

min_max index is a [local index](../concepts/glossary.md#local-index) that speeds up scanning queries with a highly selective filter by skipping fragments. Unlike global [secondary indexes](../concepts/glossary.md#secondary-index), it acts as a read filter for the base table and reduces the amount of data that actually needs to be read.

For each indexed data fragment, the min_max index stores the minimum and maximum value of one column. During query execution, {{ ydb-short-name }} evaluates the predicate on these two values. If the evaluation results show that the predicate will filter out all tuples of the fragment, the fragment is skipped.

## Examples {#examples}

Creation syntax: [CREATE TABLE](../yql/reference/syntax/create_table/min_max_index.md), [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-min-max).

Creating a columnar table with a min_max index:


```yql
CREATE TABLE events (
    id Uint64,
    created_at Timestamp,
    level Int32,
    resource_id Utf8,
    PRIMARY KEY (id),
    INDEX idx_created_at LOCAL USING min_max
        ON (created_at),
    INDEX idx_level LOCAL USING min_max
        ON (level)
)
WITH (
    STORE = COLUMN
);
```


Adding a min_max index to an existing columnar table:


```yql
ALTER TABLE events
  ADD INDEX idx_resource_id LOCAL USING min_max
  ON (resource_id);
```


Queries with range predicates can use the index to skip non-matching fragments:


```yql
SELECT id, resource_id
FROM events
WHERE created_at BETWEEN Timestamp("2024-01-01T00:00:00.000000Z")
                     AND Timestamp("2024-01-02T00:00:00.000000Z");
```


## When to use {#use}

The min_max index is useful when the values of the indexed column change little between rows that are adjacent in the primary key order: for example, timestamps, monotonically increasing identifiers, or other values correlated with the primary key.

The min_max index can also be useful when the filter predicate selects a very small fraction of the data (on the order of one row per million). For example, when querying a service log table and keeping only records with the `ERROR` level: if the service writes one error per 1,000,000 records, the min_max index will likely significantly reduce the read volume.

## Features and limitations {#limitations}

{% include [min_max_index_features.md](../yql/reference/syntax/_includes/min_max_index_features.md) %}

{% note info "Limitations" %}

{% include [min_max_index_limitations.md](../yql/reference/syntax/_includes/min_max_index_limitations.md) %}

{% endnote %}

## Additional materials {#see-also}

* [Secondary indexes](secondary-indexes.md)
* [Local indexes](../concepts/query_execution/local_indexes.md)
* [YQL reference: CREATE TABLE](../yql/reference/syntax/create_table/min_max_index.md)
* [YQL reference: SELECT](../yql/reference/syntax/select/index.md)
* [YQL reference: ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#local-min-max)
* [Quick start](../recipes/min_max-skip-index/min_max-skip-index-quickstart.md)
