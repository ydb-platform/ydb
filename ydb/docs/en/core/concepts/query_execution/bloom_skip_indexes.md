# Bloom skip indexes and filtering

Bloom skip indexes are local probabilistic filters over column data that let the storage layer discard table fragments that very likely do not contain the requested values. This reduces the amount of data actually read for selective queries.

## How they differ from global secondary and fulltext indexes

* A global [secondary index](../glossary.md#secondary-index) is a separate index structure with its own key for data access.
* A [fulltext index](../../dev/fulltext-indexes.md) is a separate inverted index over text for search and relevance ranking.
* Bloom skip data is stored locally together with table data and works as a read-time filter: it does not materialize a separate index table, but helps the [query optimizer](optimizer.md) and storage layer discard irrelevant data fragments.

## When to use which type

* `bloom_filter`: Use when queries often filter by exact values of the indexed column (equality, small sets of values).
* `bloom_ngram_filter`: Use for string columns when you need the n-gram variant of Bloom skip filtering.

For index types, limitations, and DDL examples, see [Bloom skip indexes](../../dev/bloom-skip-indexes.md) and the [YQL reference](../../yql/reference/syntax/alter_table/indexes.md#local-bloom).
