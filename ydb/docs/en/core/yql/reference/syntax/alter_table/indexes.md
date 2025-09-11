# Adding, removing, and renaming a index

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

## Adding an index {#add-index}

`ADD INDEX` — adds an index with the specified name and type for a given set of columns. Grammar:

```yql
ALTER TABLE `<table_name>`
  ADD INDEX `<index_name>`
    [GLOBAL|LOCAL]
    [UNIQUE]
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

{% if backend_name == "YDB" %}

You can also add a secondary index using the {{ ydb-short-name }} CLI [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) command.

{% endif %}

### Examples

A regular secondary index:

```yql
ALTER TABLE `series`
  ADD INDEX `title_index`
  GLOBAL ON (`title`);
```

A vector index:

```yql
ALTER TABLE `series`
  ADD INDEX emb_cosine_idx GLOBAL SYNC USING vector_kmeans_tree
  ON (embedding) COVER (title)
  WITH (
    distance="cosine",
    vector_type="float",
    vector_dimension=512,
    clusters=128,
    levels=2
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
* `<setting_name>`: The name of the setting to be modified, which should be one of the following:

    * [AUTO_PARTITIONING_BY_SIZE]({{ concept_table }}#auto_partitioning_by_size)
    * [AUTO_PARTITIONING_BY_LOAD]({{ concept_table }}#auto_partitioning_by_load)
    * [AUTO_PARTITIONING_PARTITION_SIZE_MB]({{ concept_table }}#auto_partitioning_partition_size_mb)
    * [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_min_partitions_count)
    * [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_max_partitions_count)
    * [READ_REPLICAS_SETTINGS]({{ concept_table }}#read_only_replicas)


{% note info %}


These settings cannot be reset.

{% endnote %}

* `<value>`: The new value for the setting. Possible values include:
    * `ENABLED` or `DISABLED` for the `AUTO_PARTITIONING_BY_SIZE` and `AUTO_PARTITIONING_BY_LOAD` settings
    * `"PER_AZ:<count>"` or `"ANY_AZ:<count>"` where `<count>` is the number of replicas for the `READ_REPLICAS_SETTINGS`
    * An integer of `Uint64` type for the other settings

### Example

The query in the following example enables automatic partitioning by load for the index named `title_index` of the table `series`, sets its minimum partition count to 5, and enables one follower per AZ for every partition:


```yql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5,
    READ_REPLICAS_SETTINGS = "PER_AZ:1"
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
