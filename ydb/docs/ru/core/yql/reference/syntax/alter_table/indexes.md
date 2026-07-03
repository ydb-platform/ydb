# Добавление, удаление и переименование индекса

## Добавление индекса {#add-index}

`ADD INDEX` — добавляет индекс с указанным именем и типом для заданного набора колонок в {% if backend_name == "YDB" and oss == true %}строковых таблицах.{% else %}таблицах.{% endif %} Грамматика:

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

Параметры для всех типов индексов:

* `parallel` - максимальное число параллельных обработчиков на основе [партиций](../../../../concepts/glossary.md#partition), задействованных в построении индекса (целое число между `1` и `MaxBuildIndexShardsInFlight` из `SchemeShardConfig`).
  - Если параметр не указан, сейчас используется значение по умолчанию `32` или `MaxBuildIndexShardsInFlight`, если это значение меньше. `MaxBuildIndexShardsInFlight` по умолчанию равен `1000`. В будущих версиях логика выбора параллелизма по умолчанию может быть изменена.
  - Вы можете установить меньший лимит, чтобы снизить влияние построения индекса на производительность базы данных.
  - Вы также можете установить больший лимит, чтобы ускорить построение индекса, если у вас достаточно аппаратных ресурсов.

Параметры, специфичные для векторных индексов:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}

{% note info %}

Для векторных индексов параметры `vector_type` и `vector_dimension` можно не указывать, если таблица не пуста — они определяются автоматически по содержимому строк. Параметры `levels` и `clusters` также определяются автоматически, и для них таблица может быть пустой, но делать это крайне не рекоммендуется так как дефолтные значения в этом случае `levels`=1, `clusters`=2, гораздо лучше создавать индекс по таблице куда уже загруженны данные, чтобы значения могли правильно определиться.

{% endnote %}


Параметры, специфичные для полнотекстовых индексов:

{% include [fulltext_index_parameters.md](../_includes/fulltext_index_parameters.md) %}

### Параметры локальных блум-индексов {#local-bloom}

{% include [bloom_skip_index_parameters.md](../_includes/bloom_skip_index_parameters.md) %}

{% if backend_name == "YDB" and oss == true %}

Также добавить вторичный индекс можно с помощью команды [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) {{ ydb-short-name }} CLI.

{% endif %}

### Ограничения

Операция `ADD INDEX` для создания глобальных вторичных (`GLOBAL`, `UNIQUE` и т.п.) и векторных индексов поддерживается только для строковых таблиц. Для [колоночных таблиц](../../../../concepts/datamodel/table.md#column-oriented-tables) через `ADD INDEX` [поддерживаются только локальные блум-индексы](#local-bloom).

Особенности локальных блум-индексов:

{% include [bloom_skip_index_features.md](../_includes/bloom_skip_index_features.md) %}

{% note info "Ограничения" %}

{% include [bloom_skip_index_limitations.md](../_includes/bloom_skip_index_limitations.md) %}

{% endnote %}

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
    distance="cosine", vector_type="float", vector_dimension=512
  );
```

Полнотекстовый индекс:

```yql
ALTER TABLE `series`
  ADD INDEX ft_idx GLOBAL USING fulltext_plain
  ON (title)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

[JSON-индекс](../../../../dev/json-indexes.md):

```yql
ALTER TABLE `series`
  ADD INDEX json_idx GLOBAL USING json
  ON (metadata);
```

[Блум-индекс](../../../../dev/bloom-skip-indexes.md):

```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_bloom LOCAL USING bloom_filter
  ON (resource_id)
  WITH (false_positive_probability = 0.01);
```

Блум n-граммный индекс:

```yql
ALTER TABLE `/Root/Table`
  ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );

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
  * для локальных блум-индексов (см. [Параметры локальных блум-индексов](#local-bloom)):
    * `FALSE_POSITIVE_PROBABILITY`
    * `NGRAM_SIZE` и `CASE_SENSITIVE` (только для `bloom_ngram_filter`)

{% note info %}

Операция `RESET` для `ALTER INDEX` не поддерживается.

{% endnote %}

* `<value>` - новое значение параметра. Возможные значения включают:

  * `ENABLED` или `DISABLED` для параметров `AUTO_PARTITIONING_BY_SIZE` и `AUTO_PARTITIONING_BY_LOAD`
  * `"PER_AZ:<count>"` или `"ANY_AZ:<count>"` где `<count>` — число реплик для `READ_REPLICAS_SETTINGS`
  * для остальных параметров — целое число типа `Uint64`
  * для `FALSE_POSITIVE_PROBABILITY` — число с плавающей точкой в диапазоне `(0, 1)`; меньшее значение обычно уменьшает число ложноположительных срабатываний, но увеличивает размер индекса
  * для `NGRAM_SIZE` — целое число в диапазоне от `3` до `8` (обычно рекомендуется начинать с `3`)
  * для `CASE_SENSITIVE` — `true` или `false`

### Пример

Код из следующего примера включает автоматическое партиционирование по нагрузке для индекса с именем `title_index` в таблице `series`, устанавливает минимальное количество партиций равным 5 и запускает по одной реплике в каждой зоне доступности (AZ) для каждой партиции:

```yql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5,
    READ_REPLICAS_SETTINGS = "PER_AZ:1"
);
```

Для локальных блум-индексов можно также менять специфичные для них параметры, например:

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

Это относится к глобальным вторичным индексам (скрытая индексная таблица и режим `--replace`). Локальные блум-индексы к такой атомарной замене под нагрузкой не применимы.

{% endif %}

Пример переименования индекса:

```yql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```
