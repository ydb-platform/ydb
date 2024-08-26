# Добавление, удаление и переименование вторичного индекса

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

### Добавление индекса

```ADD INDEX``` — добавляет индекс с указанным именем и типом для заданного набора колонок в {% if backend_name == "YDB" %}строковых таблицах.{% else %}таблицах.{% endif %} Приведенный ниже код добавит глобальный индекс с именем ```title_index``` для колонки ```title```.

```sql
ALTER TABLE `series` ADD INDEX `title_index` GLOBAL ON (`title`);
```

Могут быть указаны все параметры индекса, описанные в команде [`CREATE TABLE`](../create_table.md#secondary_index)

{% if backend_name == "YDB" %}

Также добавить вторичный индекс можно с помощью команды [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) {{ ydb-short-name }} CLI.

{% endif %}

### Изменение параметров индекса {#alter-index}

Индексы имеют параметры, зависящие от типа, которые можно настраивать. Глобальные индексы, [синхронные]({{ concept_secondary_index }}#sync) или [асинхронные]({{ concept_secondary_index }}#async), реализованы в виде скрытых таблиц, и их параметры автоматического партиционирования можно регулировать так же, как и настройки обычных таблиц.

{% note info %}

В настоящее время задание настроек партиционирования вторичных индексов при создании индекса не поддерживается ни в операторе [`ALTER TABLE ADD INDEX`](#add-index), ни в операторе [`CREATE TABLE INDEX`](../create_table/secondary_index.md).

{% endnote %}

```sql
ALTER TABLE <table_name> ALTER INDEX <index_name> SET <partitioning_setting_name> <value>;
ALTER TABLE <table_name> ALTER INDEX <index_name> SET (<partitioning_setting_name_1> = <value_1>, ...);
```

* `<table_name>` - имя таблицы, индекс которой нужно изменить. 
* `<index_name>` - имя индекса, который нужно изменить.
* `<partitioning_setting_name>` - имя изменяемого параметра, который должен быть одним из следующих:
  * [AUTO_PARTITIONING_BY_SIZE]({{ concept_table }}#auto_partitioning_by_size)
  * [AUTO_PARTITIONING_BY_LOAD]({{ concept_table }}#auto_partitioning_by_load)
  * [AUTO_PARTITIONING_PARTITION_SIZE_MB]({{ concept_table }}#auto_partitioning_partition_size_mb)
  * [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_min_partitions_count)
  * [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_max_partitions_count)

{% note info %}

Эти настройки нельзя вернуть к исходным.

{% endnote %}

* `<value>` - новое значение параметра. Возможные значения включают:
    * `ENABLED` или `DISABLED` для параметров `AUTO_PARTITIONING_BY_SIZE` и `AUTO_PARTITIONING_BY_LOAD`
    * для остальных параметров — целое число типа `Uint64`

#### Пример

Код из следующего примера включает автоматическое партиционирование по нагрузке для индекса с именем `title_index` в таблице `series` и устанавливает ему минимальное количество партиций равным 5:
```sql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5
);
```

### Удаление индекса {#drop-index}

```DROP INDEX``` — удаляет индекс с указанным именем. Приведенный ниже код удалит индекс с именем ```title_index```.

```sql
ALTER TABLE `series` DROP INDEX `title_index`;
```

{% if backend_name == "YDB" %}

Также удалить вторичный индекс можно с помощью команды [table index](../../../../reference/ydb-cli/commands/secondary_index.md#drop) {{ ydb-short-name }} CLI.

{% endif %}

### Переименование вторичного индекса {#rename-secondary-index}

`RENAME INDEX` — переименовывает индекс с указанным именем. Если индекс с новым именем существует, будет возвращена ошибка.

{% if backend_name == "YDB" %}

Возможность атомарной замены индекса под нагрузкой поддерживается командой [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) {{ ydb-short-name }} CLI и специализированными методами {{ ydb-short-name }} SDK.

{% endif %}

Пример переименования индекса:

```sql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```