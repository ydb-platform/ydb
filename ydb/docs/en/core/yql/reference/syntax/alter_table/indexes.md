# Adding, deleting, and renaming an index

## Adding an index {#add-index}

`ADD INDEX` — adds an index with the specified name and type for the given set of columns in {% if backend_name == "YDB" and oss == true %}row tables.{% else %}tables.{% endif %} Grammar:


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

Parameters for all index types:

* - maximum number of `parallel` handlers based on [partitions](../../../../concepts/glossary.md#partition) involved in index building (integer between `1` and `MaxBuildIndexShardsInFlight` from `SchemeShardConfig`).

- If the parameter is not specified, the default value `32` or `MaxBuildIndexShardsInFlight` is currently used, whichever is smaller. `MaxBuildIndexShardsInFlight` defaults to `1000`. In future versions, the default parallelism selection logic may change.
- You can set a lower limit to reduce the impact of index building on database performance.
- You can also set a higher limit to speed up index building if you have sufficient hardware resources.

Parameters specific to vector indexes:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}

{% note info %}

For vector indexes, parameters `vector_type` and `vector_dimension` can be omitted if the table is not empty — they are determined automatically from the row contents. Parameters `levels` and `clusters` are also determined automatically, and for them the table can be empty, but doing so is strongly not recommended because the default values in this case are `levels`=1, `clusters`=2; it is much better to create the index on a table that already has data loaded so that the values can be determined correctly.

{% endnote %}

Parameters specific to full-text indexes:

{% include [fulltext_index_parameters.md](../_includes/fulltext_index_parameters.md) %}

### Parameters of local bloom indexes {#local-bloom}

{% include [bloom_skip_index_parameters.md](../_includes/bloom_skip_index_parameters.md) %}

{% if backend_name == "YDB" and oss == true %}

You can also add a secondary index using the [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) {{ ydb-short-name }} CLI command.

{% endif %}

### Limitations

The `ADD INDEX` operation for creating global secondary (`GLOBAL`, `UNIQUE`, etc.) and vector indexes is supported only for row tables. For [columnar tables](../../../../concepts/datamodel/table.md#column-oriented-tables), via `ADD INDEX`, [only local bloom indexes are supported](#local-bloom).

Features of local bloom indexes:

{% include [bloom_skip_index_features.md](../_includes/bloom_skip_index_features.md) %}

{% note info "Limitations" %}

{% include [bloom_skip_index_limitations.md](../_includes/bloom_skip_index_limitations.md) %}

{% endnote %}

### Examples

Secondary index:


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
    distance="cosine", vector_type="float", vector_dimension=512
  );
```


Full-text index:


```yql
ALTER TABLE `series`
  ADD INDEX ft_idx GLOBAL USING fulltext_plain
  ON (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```


[JSON index](../../../../dev/json-indexes.md):


```yql
ALTER TABLE `series`
  ADD INDEX json_idx GLOBAL USING json
  ON (metadata);
```


[Bloom index](../../../../dev/bloom-skip-indexes.md):


```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_bloom LOCAL USING bloom_filter
  ON (resource_id)
  WITH (false_positive_probability = 0.01);
```


Bloom n-gram index:


```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );

# # Changing index parameters {#alter-index}

Индексы имеют параметры, зависящие от типа, которые можно настраивать. Глобальные индексы, [синхронные](yfmvar-0-yfmvarend#sync) или [асинхронные](yfmvar-1-yfmvarend#async), реализованы в виде скрытых таблиц, и их параметры автоматического партиционирования и реплик можно регулировать так же, как и настройки обычных таблиц.

{% note info %}

В настоящее время задание настроек партиционирования вторичных индексов при создании индекса не поддерживается ни в операторе [`ALTER TABLE ADD INDEX`](#add-index), ни в операторе [`CREATE TABLE INDEX`](../create_table/secondary_index.md).

{% endnote %}

```yql
ALTER TABLE <table_name> ALTER INDEX <index_name> SET <setting_name> <value>;
ALTER TABLE <table_name> ALTER INDEX <index_name> SET (<setting_name_1> = <value_1>, ...);
```


* `<table_name>` - name of the table whose index needs to be changed.
* `<index_name>` - name of the index to change.
* `<setting_name>` - name of the parameter to change. The set of allowed parameters depends on the index type:

  * for global secondary indexes:

    * [AUTO_PARTITIONING_BY_SIZE]({{ concept_table }}#auto_partitioning_by_size)
    * [AUTO_PARTITIONING_BY_LOAD]({{ concept_table }}#auto_partitioning_by_load)
    * [AUTO_PARTITIONING_PARTITION_SIZE_MB]({{ concept_table }}#auto_partitioning_partition_size_mb)
    * [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_min_partitions_count)
    * [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_max_partitions_count)
    * [READ_REPLICAS_SETTINGS]({{ concept_table }}#read_only_replicas)
  * for local bloom indexes (see [Parameters of local bloom indexes](#local-bloom)):

    * `FALSE_POSITIVE_PROBABILITY`
    * `NGRAM_SIZE` and `CASE_SENSITIVE` (only for `bloom_ngram_filter`)

{% note info %}

The `RESET` operation for `ALTER INDEX` is not supported.

{% endnote %}

* `<value>` - new parameter value. Possible values include:

  * `ENABLED` or `DISABLED` for parameters `AUTO_PARTITIONING_BY_SIZE` and `AUTO_PARTITIONING_BY_LOAD`
  * `"PER_AZ:<count>"` or `"ANY_AZ:<count>"` where `<count>` is the number of replicas for `READ_REPLICAS_SETTINGS`
  * for other parameters — an integer of type `Uint64`
  * for `FALSE_POSITIVE_PROBABILITY` — a floating-point number in the range `(0, 1)`; a smaller value usually reduces the number of false positives but increases the index size
  * for `NGRAM_SIZE` — an integer in the range from `3` to `8` (usually recommended to start with `3`)
  * for `CASE_SENSITIVE` — `true` or `false`

### Example

The code in the following example enables automatic partitioning by load for the index named `title_index` in the table `series`, sets the minimum number of partitions to 5, and starts one replica in each availability zone (AZ) for each partition:


```yql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5,
    READ_REPLICAS_SETTINGS = "PER_AZ:1"
);
```


For local bloom indexes, you can also change their specific parameters, for example:


```yql
ALTER TABLE `/Root/Table` ALTER INDEX idx_ngram SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```


## Deleting an index {#drop-index}

`DROP INDEX` — deletes the index with the specified name. The code below will delete the index named `title_index`.


```yql
ALTER TABLE `series` DROP INDEX `title_index`;
```


{% if backend_name == "YDB" and oss == true %}

You can also delete an index using the [table index](../../../../reference/ydb-cli/commands/secondary_index.md#drop) {{ ydb-short-name }} CLI command.

{% endif %}

## Renaming a secondary index {#rename-secondary-index}

`RENAME INDEX` — renames the index with the specified name. If an index with the new name already exists, an error will be returned.

{% if backend_name == "YDB" and oss == true %}

The ability to atomically replace an index under load is supported by the [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) {{ ydb-short-name }} CLI command and specialized {{ ydb-short-name }} SDK methods.

This applies to global secondary indexes (hidden index table and `--replace` mode). Local bloom indexes are not applicable to such atomic replacement under load.

{% endif %}

Example of renaming an index:


```yql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```
