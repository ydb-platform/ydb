# Bloom skip indexes

Bloom skip indexes are [local](../concepts/glossary.md#local-index) auxiliary structures based on a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that speed up selective queries by skipping data fragments that cannot contain the requested value. Unlike global [secondary indexes](../concepts/glossary.md#secondary-index), they act as read-time filters on the main table and reduce the amount of data that must be actually read.

## Types {#types}

[Local Bloom skip indexes](../concepts/glossary.md#local-bloom-skip-index) are a kind of [local index](../concepts/glossary.md#local-index). {{ ydb-short-name }} supports two types:

* `bloom_filter`: a filter over exact values of the indexed column; use for equality and `IN` (see [when to use which](../concepts/query_execution/bloom_skip_indexes.md#bloom-skip-indexes)).
* `bloom_ngram_filter`: a filter over n-grams of a string column (`String`, `Utf8`); use for substring and `LIKE` pattern search on [column-oriented tables](../concepts/glossary.md#column-oriented-table).

For type differences and comparison with global indexes, see [local indexes](../concepts/query_execution/bloom_skip_indexes.md).

## Parameters and defaults {#parameters}

Full list of `WITH (...)` parameters and defaults:

{% include [bloom_skip_index_parameters.md](../yql/reference/syntax/_includes/bloom_skip_index_parameters.md) %}

Creation syntax: [CREATE TABLE](../yql/reference/syntax/create_table/bloom_skip_index.md), [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

To change parameters after creation, use [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index).

## Examples {#examples}

Create a table with a `bloom_filter` index:

```yql
CREATE TABLE `/Root/events` (
    id Uint64,
    resource_id Utf8,
    message Utf8,
    PRIMARY KEY (id),
    INDEX idx_bloom LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
);
```

Add a `bloom_ngram_filter` index to an existing table:

```yql
ALTER TABLE `/Root/events`
  ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

Alter index parameters:

```yql
ALTER TABLE `/Root/events` ALTER INDEX idx_ngram SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```

## Parameter tuning {#tuning}

If queries still read too much data after deploying the index, try lowering `false_positive_probability`. If the index uses too much space, try raising it.

Use [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index) to change values, as in the [example above](#examples).

Tips:

* Start with `false_positive_probability = 0.01`, then adjust based on real read metrics and index size.
* For `bloom_ngram_filter`, `ngram_size` typically starts at `3`; increasing it can make filtering stricter for longer substrings.
* Change one parameter at a time and compare results under the same workload.

## Limitations {#limitations}

{% include [bloom_skip_index_limitations.md](../yql/reference/syntax/_includes/bloom_skip_index_limitations.md) %}

## Additional Materials {#see-also}

* [Secondary indexes](secondary-indexes.md)
* [YQL reference: CREATE TABLE](../yql/reference/syntax/create_table/bloom_skip_index.md)
* [YQL reference: SELECT](../yql/reference/syntax/select/index.md)
* [YQL reference: ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#local-bloom)
* [Quickstart recipe](../recipes/bloom-skip-indexes/bloom-skip-index-quickstart.md)
