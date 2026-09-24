# min_max index

A `min_max` index is a local skip index for column-oriented tables. For each
data fragment, it stores the minimum and maximum value of one column. YDB can
skip a fragment when its interval cannot satisfy the query predicate.

The `min_max` index is available starting from YDB 26.2.

{% note info %}

In YDB 26.2, this feature is gated by `enable_local_min_max_index` and is
disabled by default.

{% endnote %}

## Syntax {#syntax}

```yql
CREATE TABLE `<table_name>` (
    ...,
    INDEX `<index_name>` LOCAL USING min_max
        ON ( <index_column> )
)
WITH (
    STORE = COLUMN
);
```

The following restrictions apply:

* only column-oriented tables are supported;
* `LOCAL` is required;
* `ON (...)` must contain exactly one column;
* `COVER (...)` and additional data columns are not supported;
* the index has no index-specific `WITH (...)` parameters;
* `Json` and `JsonDocument` columns are not supported.

The optimizer can use this index with `=`, `<`, `<=`, `>`, `>=`, and `BETWEEN`
predicates and compatible combinations of them using `AND` or `OR`. Do not use
the `VIEW` syntax: the storage layer applies this local index automatically.

## Functional test example {#functional-test}

The following example creates a column-oriented table, declares the index,
writes three values, and checks a range predicate:

```yql
CREATE TABLE minmax_example (
    id Uint64 NOT NULL,
    value Int64,
    PRIMARY KEY (id),
    INDEX minmax_idx LOCAL USING min_max ON (value)
)
WITH (
    STORE = COLUMN
);

UPSERT INTO minmax_example (id, value) VALUES
    (1u, 10l),
    (2u, 20l),
    (3u, 30l);

SELECT value
FROM minmax_example
WHERE value BETWEEN 15l AND 25l;
-- Exactly one row: 20

SHOW CREATE TABLE minmax_example;
-- The returned DDL contains minmax_idx and USING min_max.

DROP TABLE minmax_example;
```

A functional test should verify both the exact query result and that the index
is present in the table schema. With only three rows, the example verifies the
schema and query correctness, but not physical fragment skipping. A performance
or pruning test needs enough data to form several fragments and must inspect
query statistics.
