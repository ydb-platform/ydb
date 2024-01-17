# ALTER TABLE

При помощи команды ```ALTER TABLE``` можно изменить состав колонок и дополнительные параметры таблицы. В одной команде можно указать несколько действий. В общем случае команда ```ALTER TABLE``` выглядит так:

```sql
ALTER TABLE table_name action1, action2, ..., actionN;
```

```action``` — это любое действие по изменению таблицы, из описанных ниже.

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

## Добавление или удаление вторичного индекса {#secondary-index}

```ADD INDEX``` — добавляет индекс с указанным именем и типом для заданного набора колонок. Приведенный ниже код добавит глобальный индекс с именем ```title_index``` для колонки ```title```.

```sql
ALTER TABLE `series` ADD INDEX `title_index` GLOBAL ON (`title`);
```

Могут быть указаны все параметры индекса, описанные в команде [`CREATE TABLE`](../create_table#secondary_index)

Удаление индекса:

```DROP INDEX``` — удаляет индекс с указанным именем. Приведенный ниже код удалит индекс с именем ```title_index```.

```sql
ALTER TABLE `series` DROP INDEX `title_index`;
```

Также добавить или удалить вторичный индекс можно с помощью команды [table index](https://ydb.tech/ru/docs/reference/ydb-cli/commands/secondary_index) {{ ydb-short-name }} CLI.

## Переименование вторичного индекса {#rename-secondary-index}

`RENAME INDEX` — переименовывает индекс с указанным именем.

Если индекс с новым именем существует, будет возвращена ошибка.

{% if backend_name == YDB %}

Возможность атомарной замены индекса под нагрузкой поддерживается командой [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) {{ ydb-short-name }} CLI и специализированными методами {{ ydb-short-name }} SDK.

{% endif %}

Пример переименования индекса:

```sql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```

{% endif %}

{% if feature_changefeed %}

## Добавление или удаление потока изменений {#changefeed}

`ADD CHANGEFEED <name> WITH (option = value[, ...])` — добавляет [поток изменений (changefeed)](../../../../concepts/cdc) с указанным именем и параметрами.

### Параметры потока изменений {#changefeed-options}

* `MODE` — режим работы. Указывает, что именно будет записано в поток при каждом изменении данных в таблице.
  * `KEYS_ONLY` — будут записаны только компоненты первичного ключа и признак изменения.
  * `UPDATES` — будут записаны значения изменившихся столбцов, получившиеся в результате изменения.
  * `NEW_IMAGE` — будут записаны значения всех столбцов, получившиеся в результате изменения.
  * `OLD_IMAGE` — будут записаны значения всех столбцов, предшествующие изменению.
  * `NEW_AND_OLD_IMAGES` - комбинация режимов `NEW_IMAGE` и `OLD_IMAGE`. Будут записаны значения всех столбцов _до_ и _в результате_ изменения.
* `FORMAT` — формат данных, в котором будут записаны данные.
  * `JSON` — записывать данные в формате [JSON](../../../../concepts/cdc#json-record-structure).
{% if audience == "tech" %}
  * `DYNAMODB_STREAMS_JSON` — записывать данные в [JSON-формате, совместимом с Amazon DynamoDB Streams](../../../../concepts/cdc#dynamodb-streams-json-record-structure).
  * `DEBEZIUM_JSON` — записывать данные в [JSON-формате, аналогичном Debezium формату](../../../../concepts/cdc#debezium-json-record-structure).
{% endif %}
* `VIRTUAL_TIMESTAMPS` — включение-выключение [виртуальных меток времени](../../../../concepts/cdc#virtual-timestamps). По умолчанию выключено.
* `RETENTION_PERIOD` — [время хранения записей](../../../../concepts/cdc#retention-period). Тип значения — `Interval`, значение по умолчанию — 24 часа (`Interval('PT24H')`).
* `TOPIC_MIN_ACTIVE_PARTITIONS` — [количество партиций топика](../../../../concepts/cdc#topic-partitions). По умолчанию количество партиций топика равно количеству партиций таблицы.
* `INITIAL_SCAN` — включение-выключение [первоначального сканирования](../../../../concepts/cdc#initial-scan) таблицы. По умолчанию выключено.
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
ALTER TABLE old_table_name RENAME TO new_table_name;
```

Если таблица с новым именем существует, будет возвращена ошибка. Возможность транзакционной подмены таблицы под нагрузкой поддерживается специализированными методами в CLI и SDK.

Если в YQL запросе содержится несколько команд `ALTER TABLE ... RENAME TO ...`, то каждая будет выполнена в режиме автокоммита в отдельной транзакции. С точки зрения внешнего процесса, таблицы будут переименованы последовательно одна за другой. Чтобы переименовать несколько таблиц в одной транзакции, используйте специализированные методы, доступные в CLI и SDK.

Переименование может использоваться для перемещения таблицы из одной директории внутри БД в другую, например:

``` sql
ALTER TABLE `table1` RENAME TO `/backup/table1`;
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

Могут быть указаны все параметры группы колонок, описанные в команде [`CREATE TABLE`](create_table#column-family)


## Изменение дополнительных параметров таблицы {#additional-alter}

Большинство параметров таблицы в YDB, приведенных на странице [описания таблицы]({{ concept_table }}), можно изменить командой ```ALTER```.

В общем случае команда для изменения любого параметра таблицы выглядит следующим образом:

```sql
ALTER TABLE table_name SET (key = value);
```

```key``` — имя параметра, ```value``` — его новое значение.

Например, такая команда выключит автоматическое партиционирование таблицы:

```sql
ALTER TABLE series SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
```

## Сброс дополнительных параметров таблицы {#additional-reset}

Некоторые параметры таблицы в YDB, приведенные на странице [описания таблицы]({{ concept_table }}), можно сбросить командой ```ALTER```.

Команда для сброса параметра таблицы выглядит следующим образом:

```sql
ALTER TABLE table_name RESET (key);
```

```key``` — имя параметра.

Например, такая команда сбросит (удалит) настройки TTL для таблицы:

```sql
ALTER TABLE series RESET (TTL);
```
{% endif %}
