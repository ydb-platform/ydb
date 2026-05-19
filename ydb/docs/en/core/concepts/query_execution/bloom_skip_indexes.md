# Local indexes

## Local and global indexes

A [local index](../glossary.md#local-index) is an auxiliary structure stored together with table data and applied while reading from storage. It does not materialize a separate index table and is not selected in queries with `VIEW` (unlike global indexes).

In {{ ydb-short-name }}, global indexes are separate structures with their own key or data representation:

* A [secondary index](../glossary.md#secondary-index) for accessing rows by values other than the primary key.
* A [vector index](../../dev/vector-indexes.md) for vector similarity search.
* A [fulltext index](../../dev/fulltext-indexes.md), an inverted index for text search.

Local indexes act as read-time filters on the main table: the [query optimizer](optimizer.md) and storage layer use them to skip irrelevant data fragments during scans.

{{ ydb-short-name }} currently implements local [Bloom skip indexes](#bloom-skip-indexes); other kinds of local indexes are planned.

## Bloom skip indexes {#bloom-skip-indexes}

Bloom skip indexes are a kind of [local index](../glossary.md#local-index) built on a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).

While reading, the index checks each data fragment to see whether the requested value (or set of n-grams) may appear there. If the filter reports that the value is definitely not present, the fragment is skipped without reading the indexed columns. If the filter does not exclude the fragment, the value may be present — including because of a false positive — and the fragment must be read. This reduces the amount of data actually read for selective queries.

### `bloom_filter` and `bloom_ngram_filter`

* `bloom_filter` builds a filter over exact values of the indexed column. Use it for equality (`=`), `IN`, and other equality comparisons on supported types.
* `bloom_ngram_filter` builds a filter over n-grams of a string column (`String`, `Utf8`). For substring or pattern search (`LIKE`), the query is split into n-grams; if a fragment's index lacks a required n-gram, the substring cannot be there and the fragment is skipped. Supported only on [column-oriented tables](../glossary.md#column-oriented-table) (see [limitations](../../yql/reference/syntax/_includes/bloom_skip_index_limitations.md)).

For parameters, see [Bloom skip indexes](../../dev/bloom-skip-indexes.md) and [CREATE TABLE](../../yql/reference/syntax/create_table/bloom_skip_index.md).

### When to use which type

* `bloom_filter`: when queries often filter by exact values of the indexed column (equality, a small set of values).
* `bloom_ngram_filter`: for string columns when substring or pattern conditions (`LIKE`) matter, not only exact equality.

### Additional materials

* [Bloom skip indexes](../../dev/bloom-skip-indexes.md)
* [YQL reference: ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom)
