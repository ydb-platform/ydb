# Bloom skip indexes and filtering

Bloom skip indexes are local probabilistic filters over column data that let the storage layer discard table fragments that very likely do not contain the requested values. This reduces the amount of data actually read for selective queries.

## How they differ from global secondary and fulltext indexes

* A global [secondary index](../glossary.md#secondary-index) is a separate structure with its own key; access through the index is usually explicit (`VIEW` for row-oriented tables in YQL).
* A [fulltext index](../../dev/fulltext-indexes.md) is a separate inverted index over text; search goes through `VIEW` and functions such as `FulltextMatch`.
* Bloom skip data is stored locally together with the table data, does not materialize rows as a separate index table, and does not require `VIEW`: the [query optimizer](optimizer.md) decides how to use the filters when evaluating predicates.

## When to use which type

* `bloom_filter`: Use when queries often filter by exact values of the indexed column (equality, small sets of values).
* `bloom_ngram_filter`: Use for string columns when substring predicates matter (including `LIKE` / `ILIKE`) and you want to skip reading irrelevant fragments.

For index types, limitations, and DDL examples, see [Bloom skip indexes](../../dev/bloom-skip-indexes.md) and the [YQL reference](../../yql/reference/syntax/alter_table/indexes.md#local-bloom).
