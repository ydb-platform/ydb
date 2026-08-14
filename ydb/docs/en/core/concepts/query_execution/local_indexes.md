# Local indexes

## Local index concept

A [local index](../glossary.md#local-index) is an auxiliary structure that is stored together with the table data and is used when reading from storage. It does not materialize a separate index table.

Local indexes act as read filters for the main table: the [query optimizer](optimizer.md) and the storage layer use them to skip irrelevant data fragments during scanning.

Currently, {{ ydb-short-name }} implements local [Bloom indexes](#bloom-skip-indexes) and a [min_max index](#min-max-index).

## Bloom indexes {#bloom-skip-indexes}

Bloom indexes are a special case of a [local index](../glossary.md#local-index) built on the [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).

When reading, for each data fragment the index checks whether the sought value (or a set of n-grams) can occur in it. If the filter reports that the value definitely does not occur, the fragment is skipped without reading the indexed columns. If the filter “passes” the check, the value may be present — including due to a false positive — and the fragment must be read. This reduces the amount of data actually read for selective queries.

### Bloom index types

* `bloom_filter` — builds a filter on the exact values of the indexed column. Suitable for equality conditions (`=`), list membership checks (`IN`), and other equality comparisons for supported types.
* `bloom_ngram_filter` — builds a filter on n-grams of a string column (`String`, `Utf8`). When searching by substring or pattern (`LIKE`), the query is split into n-grams; if the fragment index lacks at least one of the required n-grams, the sought substring cannot be in it, and the fragment is skipped. Supported only in [columnar tables](../glossary.md#column-oriented-table).

### Local Bloom indexes

Type `bloom_filter` works in both [row-based](../glossary.md#row-oriented-table) (OLTP) and [columnar](../glossary.md#column-oriented-table) (OLAP) tables, but the implementation differs:

* In row-based tables, the filter is built as a prefix Bloom filter on the left prefix of the [primary key](../glossary.md#primary-key). The indexed columns must form a continuous leading subset of the primary key columns. This speeds up point reads and range scans that restrict the leading key columns. To create a prefix Bloom filter, use [ALTER TABLE ... ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom); to drop it, use [ALTER TABLE ... DROP INDEX](../../yql/reference/syntax/alter_table/indexes.md#drop-index).
* In columnar tables, the filter is built on the values of the indexed column in each data fragment (portion) and is used in analytical scans to skip fragments that do not contain the sought value.

{% note info "Limitations" %}

{% include [bloom_skip_index_limitations.md](../../yql/reference/syntax/_includes/bloom_skip_index_limitations.md) %}

{% endnote %}

### Additional materials

* [Bloom indexes](../../dev/bloom-skip-indexes.md)
* [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom)

## min_max index {#min-max-index}

A min_max index is a special case of a [local index](../glossary.md#local-index) that stores the minimum and maximum value of one indexed column for each data fragment.

When reading with a [special kind of filter](#min-max-index-predicates) on a column with a min_max index, {{ ydb-short-name }} first reads the minimum and maximum values stored for the fragment and checks whether the filter interval intersects this range. If the intervals do not intersect, the predicate is guaranteed to be false on all values of the fragment, so the fragment can be skipped. This is useful for range predicates and equality conditions (a special case of a range predicate) on columns with a small spread between the minimum and maximum values within stored fragments.

Example:

In storage, for the [`events` table](../../yql/reference/syntax/create_table/min_max_index.md#example), one of the fragments of column `level` of type `Int32` contains values `[5, 5, 9, 5, 9, 13]`. Then the minimum value is 5, the maximum is 13. For query `SELECT * FROM events WHERE level = 15`, the filter interval is `[15, 15]`. It does not intersect with interval `[5, 13]`, so such a fragment does not need to be read from storage.

### Min-max index predicates {#min-max-index-predicates}

The optimizer can use the min_max index for predicates `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, as well as compatible combinations with `AND` or `OR`.

{% note info "Limitations" %}

{% include [min_max_index_limitations.md](../../yql/reference/syntax/_includes/min_max_index_limitations.md) %}

{% endnote %}

### Additional materials

* [min_max index](../../dev/min_max-skip-index.md)
* [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-min-max)
