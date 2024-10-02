# Добавление или удаление потока изменений

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

`ADD CHANGEFEED <name> WITH (option = value[, ...])` — добавляет [поток изменений (changefeed)](../../../../concepts/cdc.md) с указанным именем и параметрами.

## Параметры потока изменений {#changefeed-options}

* `MODE` — режим работы. Указывает, что именно будет записано в поток при каждом изменении данных в таблице:
    * `KEYS_ONLY` — будут записаны только компоненты первичного ключа и признак изменения.
    * `UPDATES` — будут записаны значения изменившихся столбцов, получившиеся в результате изменения.
    * `NEW_IMAGE` — будут записаны значения всех столбцов, получившиеся в результате изменения.
    * `OLD_IMAGE` — будут записаны значения всех столбцов, предшествующие изменению.
    * `NEW_AND_OLD_IMAGES` - комбинация режимов `NEW_IMAGE` и `OLD_IMAGE`. Будут записаны значения всех столбцов _до_ и _в результате_ изменения.
* `FORMAT` — формат данных, в котором будут записаны данные:
    * `JSON` — записывать данные в формате [JSON](../../../../concepts/cdc.md#json-record-structure).
    * `DYNAMODB_STREAMS_JSON` — записывать данные в [JSON-формате, совместимом с Amazon DynamoDB Streams](../../../../concepts/cdc.md#dynamodb-streams-json-record-structure) JSON-формате, совместимом с Amazon DynamoDB Streams.
    * `DEBEZIUM_JSON` — записывать данные в [JSON-формате, аналогичном Debezium формату](../../../../concepts/cdc.md#debezium-json-record-structure).
* `VIRTUAL_TIMESTAMPS` — включение-выключение [виртуальных меток времени](../../../../concepts/cdc.md#virtual-timestamps).
* `RETENTION_PERIOD` — [время хранения записей](../../../../concepts/cdc.md#retention-period). Тип значения — `Interval`, значение по умолчанию — 24 часа (`Interval('PT24H')`).
* `TOPIC_MIN_ACTIVE_PARTITIONS` — [количество партиций топика](../../../../concepts/cdc.md#topic-partitions). По умолчанию количество партиций топика равно количеству партиций таблицы.
* `INITIAL_SCAN` — включение-выключение [первоначального сканирования](../../../../concepts/cdc.md#initial-scan) таблицы. По умолчанию выключено.

{% if audience == "tech" %}

* `AWS_REGION` — значение, которое будет записано в поле `awsRegion`. Применимо только совместно с форматом `DYNAMODB_STREAMS_JSON`.

{% endif %}

Приведенный ниже код добавит поток изменений с именем `updates_feed`, в который будут выгружаться значения изменившихся столбцов таблицы в формате JSON:

```yql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES'
);
```

Записи в таком потоке изменений будут храниться в течение 24 часов (значение по умолчанию). Код из следующего примера создаст поток изменений с хранением записей в течение 12 часов:

```yql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    RETENTION_PERIOD = Interval('PT12H')
);
```

Пример создания потока изменений с включенными виртуальными метками времени:

```yql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    VIRTUAL_TIMESTAMPS = TRUE
);
```

Пример создания потока изменений с первоначальным сканированием:

```yql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    INITIAL_SCAN = TRUE
);
```

`DROP CHANGEFEED` — удаляет поток изменений с указанным именем. Приведенный ниже код удалит changefeed с именем `updates_feed`:

```yql
ALTER TABLE `series` DROP CHANGEFEED `updates_feed`;
```