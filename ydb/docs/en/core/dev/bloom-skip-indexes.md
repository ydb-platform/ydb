# Bloom skip indexes

Bloom skip indexes are local auxiliary structures that speed up selective queries by skipping data fragments that very likely do not contain the requested values. Unlike global [secondary indexes](../concepts/glossary.md#secondary-index), they work as local read-time filters and reduce the amount of data that must be actually read.

For how they fit into query execution, see [Bloom skip indexes and filtering](../concepts/query_execution/bloom_skip_indexes.md).

## Types {#types}

{{ ydb-short-name }} supports two local Bloom skip index types:

* `bloom_filter`: A Bloom filter over exact values of the indexed column (useful for equality and point lookups).
* `bloom_ngram_filter`: A Bloom filter over n-grams of a string column.

Creation syntax and parameters are documented in [CREATE TABLE: Bloom skip index](../yql/reference/syntax/create_table/bloom_skip_index.md) and in [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

## Limitations {#limitations}

* The index is always local (`LOCAL`); there is no global variant.
* `COVER (...)` and data columns are not supported.
* For column-oriented tables, exactly one indexed column is allowed. For row-oriented tables, multiple indexed columns are allowed.
* Queries do not use the `VIEW <index>` syntax (unlike, for example, [fulltext indexes](fulltext-indexes.md)).

## Parameters and defaults {#parameters}

Summary of `WITH (...)` parameters and defaults:

{% include [bloom_skip_index_parameters.md](../yql/reference/syntax/_includes/bloom_skip_index_parameters.md) %}

To change parameters after creation, use [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index).

## Examples {#examples}

Create a table with a `bloom_filter` index:

```yql
CREATE TABLE events (
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

## Additional Materials {#see-also}

* [Secondary indexes](secondary-indexes.md)
* [YQL reference: `CREATE TABLE` / Bloom skip index](../yql/reference/syntax/create_table/bloom_skip_index.md)
* [YQL reference: `SELECT`](../yql/reference/syntax/select/index.md)
* [YQL reference: `ALTER TABLE` / indexes](../yql/reference/syntax/alter_table/indexes.md#local-bloom)
* [Recipes: Bloom skip indexes](../recipes/bloom-skip-indexes/index.md)
