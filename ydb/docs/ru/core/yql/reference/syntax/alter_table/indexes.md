# Добавление, удаление и переименование индекса

## Добавление индекса {#add-index}

`ADD INDEX` — добавляет индекс с указанным именем и типом для заданного набора колонок в {% if backend_name == "YDB" and oss == true %}строковых таблицах.{% else %}таблицах.{% endif %} Грамматика:

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

Параметры, специфичные для векторных индексов:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}

Параметры, специфичные для полнотекстовых индексов:

{% include [fulltext_index_parameters.md](../_includes/fulltext_index_parameters.md) %}

{% if backend_name == "YDB" and oss == true %}

Также добавить вторичный индекс можно с помощью команды [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) {{ ydb-short-name }} CLI.

{% endif %}

### Ограничения

Операция `ADD INDEX` для создания глобальных вторичных (`GLOBAL`, `UNIQUE` и т.п.) и векторных индексов поддерживается только для строковых таблиц. Для [колоночных таблиц](../../../../concepts/datamodel/table.md#column-oriented-tables) через `ADD INDEX` поддерживаются только локальные Bloom skip индексы, см. [Bloom skip индексы](#local-bloom).

### Примеры

Вторичный индекс:

```yql
ALTER TABLE `series`
  ADD INDEX `title_index`
  GLOBAL ON (`title`);
```

[Векторный индекс](../../../../dev/vector-indexes.md):

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

Полнотекстовый индекс:

```yql
ALTER TABLE `series`
  ADD INDEX ft_idx GLOBAL USING fulltext_plain
  ON (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

### Bloom skip индексы {#local-bloom}

Bloom skip индексы позволяют пропускать фрагменты данных, которые не содержат искомых значений, и ускоряют селективные запросы. Обзор и сценарии использования см. в разделе [Bloom skip индексы](../../../../dev/bloom-skip-indexes.md).

Для локальных Bloom skip индексов действуют дополнительные ограничения:

* для колоночных таблиц выражение `ON (...)` должно содержать одну колонку;
* секции `COVER (...)` и data columns для таких индексов не поддерживаются.

Поддерживаются следующие типы индексов:

* `bloom_filter` — фильтр Блума по значениям колонки. Параметры:
  * `false_positive_probability` — целевая вероятность ложноположительного срабатывания (например, `0.01`). По умолчанию: `0.1` для колоночных таблиц и `0.0001` для строковых таблиц.
* `bloom_ngram_filter` — n-граммный фильтр Блума для колонок строковых типов. Параметры:
  * `ngram_size` — размер n-граммы от `3` до `8` (например, `3`). По умолчанию: `3`;
  * `false_positive_probability` — целевая вероятность ложноположительного срабатывания (например, `0.01`). По умолчанию: `0.1`;
  * `case_sensitive` — необязательный, `true` или `false` (по умолчанию `true`).

#### Пример: добавление индекса Bloom filter

```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_bloom LOCAL USING bloom_filter
  ON (resource_id)
  WITH (false_positive_probability = 0.01);
```

#### Пример: добавление индекса N-gram Bloom filter

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

## Изменение параметров индекса {#alter-index}

Индексы имеют параметры, зависящие от типа, которые можно настраивать. Глобальные индексы, [синхронные]({{ concept_secondary_index }}#sync) или [асинхронные]({{ concept_secondary_index }}#async), реализованы в виде скрытых таблиц, и их параметры автоматического партиционирования и реплик можно регулировать так же, как и настройки обычных таблиц.

{% note info %}

В настоящее время задание настроек партиционирования вторичных индексов при создании индекса не поддерживается ни в операторе [`ALTER TABLE ADD INDEX`](#add-index), ни в операторе [`CREATE TABLE INDEX`](../create_table/secondary_index.md).

{% endnote %}

```yql
ALTER TABLE <table_name> ALTER INDEX <index_name> SET <setting_name> <value>;
ALTER TABLE <table_name> ALTER INDEX <index_name> SET (<setting_name_1> = <value_1>, ...);
```

* `<table_name>` - имя таблицы, индекс которой нужно изменить.
* `<index_name>` - имя индекса, который нужно изменить.
* `<setting_name>` - имя изменяемого параметра. Набор допустимых параметров зависит от типа индекса:
  * для глобальных вторичных индексов:

    * [AUTO_PARTITIONING_BY_SIZE]({{ concept_table }}#auto_partitioning_by_size)
    * [AUTO_PARTITIONING_BY_LOAD]({{ concept_table }}#auto_partitioning_by_load)
    * [AUTO_PARTITIONING_PARTITION_SIZE_MB]({{ concept_table }}#auto_partitioning_partition_size_mb)
    * [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_min_partitions_count)
    * [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_max_partitions_count)
    * [READ_REPLICAS_SETTINGS]({{ concept_table }}#read_only_replicas)
  * для локальных Bloom skip индексов:
    * `false_positive_probability`
    * `ngram_size` и `case_sensitive` (только для `bloom_ngram_filter`)

{% note info %}

Для параметров глобальных вторичных индексов операция `RESET` не поддерживается.

{% endnote %}

* `<value>` - новое значение параметра. Возможные значения включают:

  * `ENABLED` или `DISABLED` для параметров `AUTO_PARTITIONING_BY_SIZE` и `AUTO_PARTITIONING_BY_LOAD`
  * `"PER_AZ:<count>"` или `"ANY_AZ:<count>"` где `<count>` — число реплик для `READ_REPLICAS_SETTINGS`
  * для остальных параметров — целое число типа `Uint64`
  * для `false_positive_probability` — число с плавающей точкой в диапазоне `(0, 1)`; меньшее значение обычно уменьшает число ложноположительных срабатываний, но увеличивает размер индекса
  * для `ngram_size` — целое число в диапазоне от `3` до `8` (обычно рекомендуется начинать с `3`)
  * для `case_sensitive` — `true` или `false`

### Пример

Код из следующего примера включает автоматическое партиционирование по нагрузке для индекса с именем `title_index` в таблице `series`, устанавливает минимальное количество партиций равным 5 и запускает по одной реплике в каждой зоне доступности (AZ) для каждой партиции:

```yql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5,
    READ_REPLICAS_SETTINGS = "PER_AZ:1"
);
```

Для локальных Bloom skip индексов можно изменять типовые параметры индекса, например:

```yql
ALTER TABLE `/Root/Table` ALTER INDEX idx_ngram SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```

## Удаление индекса {#drop-index}

`DROP INDEX` — удаляет индекс с указанным именем. Приведенный ниже код удалит индекс с именем `title_index`.

```yql
ALTER TABLE `series` DROP INDEX `title_index`;
```

{% if backend_name == "YDB" and oss == true %}

Также удалить индекс можно с помощью команды [table index](../../../../reference/ydb-cli/commands/secondary_index.md#drop) {{ ydb-short-name }} CLI.

{% endif %}

## Переименование вторичного индекса {#rename-secondary-index}

`RENAME INDEX` — переименовывает индекс с указанным именем. Если индекс с новым именем существует, будет возвращена ошибка.

{% if backend_name == "YDB" and oss == true %}

Возможность атомарной замены индекса под нагрузкой поддерживается командой [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) {{ ydb-short-name }} CLI и специализированными методами {{ ydb-short-name }} SDK.

{% endif %}

Пример переименования индекса:

```yql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```
