# ALTER TABLE

При помощи команды ```ALTER TABLE``` можно изменить состав колонок и дополнительные параметры таблицы. В одной команде можно указать несколько действий. В общем случае команда ```ALTER TABLE``` выглядит так:

```sql
ALTER TABLE <table_name> <action1>, <action2>, ..., <actionN>;
```

```<action>``` — это любое действие по изменению таблицы, из описанных ниже.

## Изменение состава колонок {#columns}

{{ backend_name }} поддерживает возможность добавлять столбцы в таблицу, а также удалять неключевые колонки из таблицы.

```ADD COLUMN``` — добавляет столбец с указанными именем и типом. Приведенный ниже код добавит к таблице ```episodes``` столбец ```is_deleted``` с типом данных ```Bool```.

```sql
ALTER TABLE episodes ADD COLUMN is_deleted Bool;
```

```DROP COLUMN``` — удаляет столбец с указанным именем. Приведенный ниже код удалит столбец ```is_deleted``` из таблицы ```episodes```.

```sql
ALTER TABLE episodes DROP column is_deleted;
```

{% if feature_secondary_index %}

## Вторичные индексы {#secondary-index}

### Добавление индекса {#add-index}

```ADD INDEX``` — добавляет индекс с указанным именем и типом для заданного набора колонок. Приведенный ниже код добавит глобальный индекс с именем ```title_index``` для колонки ```title```.

```sql
ALTER TABLE `series` ADD INDEX `title_index` GLOBAL ON (`title`);
```

Могут быть указаны все параметры индекса, описанные в команде [`CREATE TABLE`](../create_table/secondary_index.md)

{% if backend_name == "YDB" %}

Также добавить вторичный индекс можно с помощью команды {% if oss == "true" %}[table index](../../../../reference/ydb-cli/commands/secondary_index.md#add){% else %}table index{% endif %} {{ ydb-short-name }} CLI.

{% endif %}

### Изменение параметров индекса {#alter-index}

Индексы имеют параметры, зависящие от типа, которые можно настраивать. Глобальные индексы, [синхронные]({{ concept_secondary_index }}#sync) или [асинхронные]({{ concept_secondary_index }}#async), реализованы в виде скрытых таблиц, и их параметры автоматического партиционирования можно регулировать так же, как и [настройки обычных таблиц](#additional-alter).

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

Эти настройки нельзя [вернуть к исходным](#additional-reset).

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

Также удалить вторичный индекс можно с помощью команды {% if oss == "true" %}[table index](../../../../reference/ydb-cli/commands/secondary_index.md#drop){% else %}table index{% endif %} {{ ydb-short-name }} CLI.

{% endif %}

### Переименование индекса {#rename-index}

`RENAME INDEX` — переименовывает индекс с указанным именем.

Если индекс с новым именем существует, будет возвращена ошибка.

{% if backend_name == "YDB" %}

Возможность атомарной замены индекса под нагрузкой поддерживается командой {% if oss == "true" %}[{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename){% else %}{{ ydb-cli }} table index rename{% endif %} {{ ydb-short-name }} CLI и специализированными методами {{ ydb-short-name }} SDK.

{% endif %}

Пример переименования индекса:

```sql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```

{% endif %}

{% if feature_changefeed %}

## Добавление или удаление потока изменений {#changefeed}

`ADD CHANGEFEED <name> WITH (<option> = <value>[, ...])` — добавляет {% if oss == "true" %}[поток изменений (changefeed)](../../../../concepts/cdc){% else %}поток изменений (changefeed){% endif %} с указанным именем и параметрами.

### Параметры потока изменений {#changefeed-options}

* `MODE` — режим работы. Указывает, что именно будет записано в поток при каждом изменении данных в таблице.
  * `KEYS_ONLY` — будут записаны только компоненты первичного ключа и признак изменения.
  * `UPDATES` — будут записаны значения изменившихся столбцов, получившиеся в результате изменения.
  * `NEW_IMAGE` — будут записаны значения всех столбцов, получившиеся в результате изменения.
  * `OLD_IMAGE` — будут записаны значения всех столбцов, предшествующие изменению.
  * `NEW_AND_OLD_IMAGES` - комбинация режимов `NEW_IMAGE` и `OLD_IMAGE`. Будут записаны значения всех столбцов _до_ и _в результате_ изменения.
* `FORMAT` — формат данных, в котором будут записаны данные.
  * `JSON` — записывать данные в формате {% if oss == "true" %}[JSON](../../../../concepts/cdc.md#json-record-structure){% else %}JSON{% endif %}.
  * `DEBEZIUM_JSON` — записывать данные в {% if oss == "true" %}[JSON-формате, аналогичном Debezium формату](../../../../concepts/cdc.md#debezium-json-record-structure){% else %}JSON-формате, аналогичном Debezium формату{% endif %}.
{% if audience == "tech" %}
  * `DYNAMODB_STREAMS_JSON` — записывать данные в {% if oss == "true" %}[JSON-формате, совместимом с Amazon DynamoDB Streams](../../../../concepts/cdc#dynamodb-streams-json-record-structure){% else %}JSON-формате, совместимом с Amazon DynamoDB Streams{% endif %}.
{% endif %}
* `VIRTUAL_TIMESTAMPS` — включение-выключение {% if oss == "true" %}[виртуальных меток времени](../../../../concepts/cdc.md#virtual-timestamps){% else %}виртуальных меток времени{% endif %}. По умолчанию выключено.
* `RETENTION_PERIOD` — {% if oss == "true" %}[время хранения записей](../../../../concepts/cdc.md#retention-period){% else %}время хранения записей{% endif %}. Тип значения — `Interval`, значение по умолчанию — 24 часа (`Interval('PT24H')`).
* `TOPIC_MIN_ACTIVE_PARTITIONS` — {% if oss == "true" %}[количество партиций топика](../../../../concepts/cdc.md#topic-partitions){% else %}количество партиций топика{% endif %}. По умолчанию количество партиций топика равно количеству партиций таблицы.
* `INITIAL_SCAN` — включение-выключение {% if oss == "true" %}[первоначального сканирования](../../../../concepts/cdc.md#initial-scan){% else %}первоначального сканирования{% endif %} таблицы. По умолчанию выключено.
{% if audience == "tech" %}
* `AWS_REGION` — значение, которое будет записано в поле `awsRegion`. Применимо только совместно с форматом `DYNAMODB_STREAMS_JSON`.
{% endif %}

Приведенный ниже код добавит поток изменений с именем `updates_feed`, в который будут выгружаться значения изменившихся столбцов таблицы в формате JSON:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES'
);
```

Записи в таком потоке изменений будут храниться в течение 24 часов (значение по умолчанию). Код из следующего примера создаст поток изменений с хранением записей в течение 12 часов:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    RETENTION_PERIOD = Interval('PT12H')
);
```

Пример создания потока изменений с включенными виртуальными метками времени:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    VIRTUAL_TIMESTAMPS = TRUE
);
```

Пример создания потока изменений с первоначальным сканированием:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    INITIAL_SCAN = TRUE
);
```

`DROP CHANGEFEED` — удаляет поток изменений с указанным именем. Приведенный ниже код удалит changefeed с именем `updates_feed`:

```sql
ALTER TABLE `series` DROP CHANGEFEED `updates_feed`;
```

{% endif %}

{% if feature_map_tables %}

## Переименование таблицы {#rename}

```sql
ALTER TABLE <old_table_name> RENAME TO <new_table_name>;
```

Если таблица с новым именем существует, будет возвращена ошибка. Возможность транзакционной подмены таблицы под нагрузкой поддерживается специализированными методами в CLI и SDK.

Если в YQL запросе содержится несколько команд `ALTER TABLE ... RENAME TO ...`, то каждая будет выполнена в режиме автокоммита в отдельной транзакции. С точки зрения внешнего процесса, таблицы будут переименованы последовательно одна за другой. Чтобы переименовать несколько таблиц в одной транзакции, используйте специализированные методы, доступные в CLI и SDK.

Переименование может использоваться для перемещения таблицы из одной директории внутри БД в другую, например:

``` sql
ALTER TABLE `table1` RENAME TO `backup/table1`;
```

## Изменение групп колонок {#column-family}

```ADD FAMILY``` — создаёт новую группу колонок в таблице. Приведенный ниже код создаст в таблице ```series_with_families``` группу колонок ```family_small```.

```sql
ALTER TABLE series_with_families ADD FAMILY family_small (
    DATA = "ssd",
    COMPRESSION = "off"
);
```

При помощи команды ```ALTER COLUMN``` можно изменить группу колонок для указанной колонки. Приведенный ниже код для колонки ```release_date``` в таблице ```series_with_families``` сменит группу колонок на ```family_small```.

```sql
ALTER TABLE series_with_families ALTER COLUMN release_date SET FAMILY family_small;
```

Две предыдущие команды из листингов 8 и 9 можно объединить в один вызов ```ALTER TABLE```. Приведенный ниже код создаст в таблице ```series_with_families``` группу колонок ```family_small``` и установит её для колонки ```release_date```.

```sql
ALTER TABLE series_with_families
    ADD FAMILY family_small (
        DATA = "ssd",
        COMPRESSION = "off"
    ),
    ALTER COLUMN release_date SET FAMILY family_small;
```

При помощи команды ```ALTER FAMILY``` можно изменить параметры группы колонок. Приведенный ниже код для группы колонок ```default``` в таблице ```series_with_families``` сменит тип хранилища на ```hdd```:

```sql
ALTER TABLE series_with_families ALTER FAMILY default SET DATA "hdd";
```

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}

Могут быть указаны все параметры группы колонок, описанные в команде [`CREATE TABLE`](../create_table/family.md)


## Изменение дополнительных параметров таблицы {#additional-alter}

Большинство параметров таблицы в YDB, приведенных на странице [описания таблицы]({{ concept_table }}), можно изменить командой ```ALTER```.

В общем случае команда для изменения любого параметра таблицы выглядит следующим образом:

```sql
ALTER TABLE <table_name> SET (<key> = <value>);
```

```<key>``` — имя параметра, ```<value>``` — его новое значение.

Например, такая команда выключит автоматическое партиционирование таблицы:

```sql
ALTER TABLE series SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
```

## Сброс дополнительных параметров таблицы {#additional-reset}

Некоторые параметры таблицы в YDB, приведенные на странице [описания таблицы]({{ concept_table }}), можно сбросить командой ```ALTER```.

Команда для сброса параметра таблицы выглядит следующим образом:

```sql
ALTER TABLE <table_name> RESET (<key>);
```

```<key>``` — имя параметра.

Например, такая команда сбросит (удалит) настройки TTL для таблицы:

```sql
ALTER TABLE series RESET (TTL);
```
{% endif %}
