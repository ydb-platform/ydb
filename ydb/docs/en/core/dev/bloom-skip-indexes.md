# Bloom skip indexes

Bloom skip indexes are local auxiliary structures that speed up selective queries by skipping data fragments that very likely do not contain the requested values. Unlike global [secondary indexes](../concepts/glossary.md#secondary-index), they work as local read-time filters and reduce the amount of data that must be actually read.

## Types {#types}

{{ ydb-short-name }} supports two local Bloom skip index types:

* `bloom_filter`: A Bloom filter over exact values of the indexed column (useful for equality and point lookups).
* `bloom_ngram_filter`: A Bloom filter over n-grams of a string column.

Creation syntax and parameters are documented in [CREATE TABLE](../yql/reference/syntax/create_table/bloom_skip_index.md) and in [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

## Parameters and defaults {#parameters}

Summary of `WITH (...)` parameters and defaults:

{% include [bloom_skip_index_parameters.md](../yql/reference/syntax/_includes/bloom_skip_index_parameters.md) %}

Detailed parameter description for index creation: [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

To change parameters after creation, use [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index).

## Examples {#examples}

Create a table with a `bloom_filter` index:

```yql
CREATE TABLE /Root/events (
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
* [YQL reference: CREATE TABLE — Bloom skip index](../yql/reference/syntax/create_table/bloom_skip_index.md)
* [YQL reference: SELECT](../yql/reference/syntax/select/index.md)
* [YQL reference: ALTER TABLE — indexes](../yql/reference/syntax/alter_table/indexes.md#local-bloom)
* [Quickstart recipe](../recipes/bloom-skip-indexes/bloom-skip-index-quickstart.md)
