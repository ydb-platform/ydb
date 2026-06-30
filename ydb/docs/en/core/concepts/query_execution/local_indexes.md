# Local indexes

## What is a local index

A [local index](../glossary.md#local-index) is an auxiliary structure stored together with table data and applied while reading from storage. It does not materialize a separate index table.

Local indexes act as read-time filters on the main table: the [query optimizer](optimizer.md) and storage layer use them to skip irrelevant data fragments during scans.

{{ ydb-short-name }} currently implements local [Bloom skip indexes](#bloom-skip-indexes); other kinds of local indexes are planned.

## Bloom skip indexes {#bloom-skip-indexes}

Bloom skip indexes are a kind of [local index](../glossary.md#local-index) built on a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).

While reading, the index checks each data fragment to see whether the requested value (or set of n-grams) may appear there. If the filter reports that the value is definitely not present, the fragment is skipped without reading the indexed columns. If the filter does not exclude the fragment, the value may be present — including because of a false positive — and the fragment must be read. This reduces the amount of data actually read for selective queries.

### Bloom skip index types

* `bloom_filter` builds a filter over exact values of the indexed column. Use it for equality (`=`), `IN`, and other equality comparisons on supported types.
* `bloom_ngram_filter` builds a filter over n-grams of a string column (`String`, `Utf8`). For substring or pattern search (`LIKE`), the query is split into n-grams; if a fragment's index lacks a required n-gram, the substring cannot be there and the fragment is skipped. Supported only on [column-oriented tables](../glossary.md#column-oriented-table).

### Local bloom skip indexes

The `bloom_filter` type works on both [row-oriented](../glossary.md#row-oriented-table) (OLTP) and [column-oriented](../glossary.md#column-oriented-table) (OLAP) tables, but the implementation differs:

* On row-oriented tables, the filter is built as a prefix bloom filter over a left prefix of the [primary key](../glossary.md#primary-key). The indexed columns must be a contiguous leading subset of the primary key columns. This accelerates point lookups and range scans that constrain the leading key columns. Use [ALTER TABLE ... ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom) to create a prefix bloom filter and [ALTER TABLE ... DROP INDEX](../../yql/reference/syntax/alter_table/indexes.md#drop-index) to remove it.
* On column-oriented tables, the filter is built over the indexed column values in each data fragment (portion) and is applied during analytical scans to skip fragments that cannot contain the requested value.

{% note info "Limitations" %}

{% include [bloom_skip_index_limitations.md](../../yql/reference/syntax/_includes/bloom_skip_index_limitations.md) %}

{% endnote %}

### Additional materials

* [Bloom skip indexes](../../dev/bloom-skip-indexes.md)
* [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom)
