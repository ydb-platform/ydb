# Adding, removing, and renaming a index

## Adding an index {#add-index}

`ADD INDEX` — adds an index with the specified name and type for a given set of columns. Grammar:

```yql
ALTER TABLE `<table_name>`
  ADD INDEX `<index_name>`
    [GLOBAL|LOCAL]
    [SYNC|ASYNC]
    [USING <index_type>]
    ON ( <index_columns> )
    [COVER ( <cover_columns> )]
    [WITH ( <parameter_name> = <parameter_value>[, ...])]
  [,   ...]
```

{% include [index_grammar_explanation.md](../_includes/index_grammar_explanation.md) %}

Parameters specific to vector indexes:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}

Parameters specific to fulltext indexes:

{% include [fulltext_index_parameters.md](../_includes/fulltext_index_parameters.md) %}

{% if backend_name == "YDB" %}

You can also add a secondary index using the {{ ydb-short-name }} CLI [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) command.

{% endif %}

### Limitations

The `ADD INDEX` operation for creating global secondary indexes (`GLOBAL`, `UNIQUE`, and so on) is supported only for row-oriented tables. For column-oriented tables, `ADD INDEX` supports only the special local Bloom skip indexes described below.

For [column-oriented tables](../../../../concepts/datamodel/table.md#column-oriented-tables), adding **global secondary** and **vector** indexes is not yet supported (see the note above). At the same time, adding **local Bloom skip indexes** is supported; see [Bloom skip indexes](#local-bloom).

### Examples

A regular secondary index:

```yql
ALTER TABLE `series`
  ADD INDEX `title_index`
  GLOBAL ON (`title`);
```

[Vector index](../../../../dev/vector-indexes.md):

```yql
ALTER TABLE `series`
  ADD INDEX emb_cosine_idx GLOBAL SYNC USING vector_kmeans_tree
  ON (embedding) COVER (title)
  WITH (
    distance="cosine",
    vector_type="float",
    vector_dimension=512,
    clusters=128,
    levels=2,
    overlap_clusters=3
  );
```

A fulltext index:

```yql
ALTER TABLE `series`
  ADD INDEX ft_idx GLOBAL USING fulltext_plain
  ON (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

### Bloom skip indexes {#local-bloom}

Bloom skip indexes allow the engine to skip data fragments that do not contain the requested values and speed up selective queries. For an overview and usage patterns, see [Bloom skip indexes](../../../../dev/bloom-skip-indexes.md).

Local Bloom skip indexes have additional limitations:

* For column-oriented tables, the `ON (...)` expression must contain exactly one column.
* `COVER (...)` and data columns are not supported for these indexes.

Supported index types:

* `bloom_filter`: Bloom filter by column values. Parameters:
  * `false_positive_probability`: Target false-positive probability (for example, `0.01`). Default: `0.1` for column-oriented tables and `0.0001` for row-oriented tables.
* `bloom_ngram_filter`: N-gram Bloom filter for string columns. Parameters:
  * `ngram_size`: N-gram size from `3` to `8` (for example, `3`). Default: `3`.
  * `false_positive_probability`: Target false-positive probability (for example, `0.01`). Default: `0.1`.
  * `case_sensitive`: Optional, `true` or `false` (`true` by default).

#### Example: add a Bloom filter index

```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_bloom LOCAL USING bloom_filter
  ON (resource_id)
  WITH (false_positive_probability = 0.01);
```

#### Example: add an N-gram Bloom filter index

```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
  ON (resource_id)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

## Altering an index {#alter-index}

Indexes have type-specific parameters that can be tuned. Global indexes, whether [synchronous]({{ concept_secondary_index }}#sync) or [asynchronous]({{ concept_secondary_index }}#async), are implemented as hidden tables, and their automatic partitioning and followers settings can be adjusted just like those of regular tables.

{% note info %}

Currently, specifying secondary index partitioning settings during index creation is not supported in either the [`ALTER TABLE ADD INDEX`](#add-index) or the [`CREATE TABLE INDEX`](../create_table/secondary_index.md) statements.

{% endnote %}

```sql
ALTER TABLE <table_name> ALTER INDEX <index_name> SET <setting_name> <value>;
ALTER TABLE <table_name> ALTER INDEX <index_name> SET (<setting_name_1> = <value_1>, ...);
```

* `<table_name>`: The name of the table whose index is to be modified.
* `<index_name>`: The name of the index to be modified.
* `<setting_name>`: The name of the setting to be modified. Allowed settings depend on the index type:
    * for global secondary indexes:
        * [AUTO_PARTITIONING_BY_SIZE]({{ concept_table }}#auto_partitioning_by_size)
        * [AUTO_PARTITIONING_BY_LOAD]({{ concept_table }}#auto_partitioning_by_load)
        * [AUTO_PARTITIONING_PARTITION_SIZE_MB]({{ concept_table }}#auto_partitioning_partition_size_mb)
        * [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_min_partitions_count)
        * [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_max_partitions_count)
        * [READ_REPLICAS_SETTINGS]({{ concept_table }}#read_only_replicas)
    * for local Bloom skip indexes:
        * `false_positive_probability`
        * `ngram_size` and `case_sensitive` (for `bloom_ngram_filter` only)


{% note info %}

For global secondary index settings, `RESET` is not supported.

{% endnote %}

* `<value>`: The new value for the setting. Possible values include:
    * `ENABLED` or `DISABLED` for the `AUTO_PARTITIONING_BY_SIZE` and `AUTO_PARTITIONING_BY_LOAD` settings
    * `"PER_AZ:<count>"` or `"ANY_AZ:<count>"` where `<count>` is the number of replicas for the `READ_REPLICAS_SETTINGS`
    * An integer of `Uint64` type for the other settings
    * A floating-point value in `(0, 1)` for `false_positive_probability`; smaller values usually reduce false positives but increase index size
    * An integer value from `3` to `8` for `ngram_size` (a typical starting point is `3`)
    * `true` or `false` for `case_sensitive`

### Example

The query in the following example enables automatic partitioning by load for the index named `title_index` of the table `series`, sets its minimum partition count to 5, and enables one follower per AZ for every partition:


```yql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5,
    READ_REPLICAS_SETTINGS = "PER_AZ:1"
);
```

For local Bloom skip indexes, you can also alter index-specific parameters, for example:

```yql
ALTER TABLE `/Root/Table` ALTER INDEX idx_ngram SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```

## Deleting an index {#drop-index}

`DROP INDEX`: Deletes the index with the specified name. The code below deletes the index named `title_index`.

```yql
ALTER TABLE `series` DROP INDEX `title_index`;
```

{% if backend_name == "YDB" %}

You can also remove a index using the {{ ydb-short-name }} CLI [table index](../../../../reference/ydb-cli/commands/secondary_index.md#drop) command.

{% endif %}

## Renaming an index {#rename-index}

`RENAME INDEX`: Renames the index with the specified name.

If an index with the new name exists, an error is returned.

{% if backend_name == "YDB" %}

Replacement of atomic indexes under load is supported by the command [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) in the {{ ydb-short-name }} CLI and by {{ ydb-short-name }} SDK ad-hoc methods.

{% endif %}

Example of index renaming:

```yql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```
