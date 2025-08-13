# Добавление или удаление потока изменений

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

`ADD CHANGEFEED <name> WITH (option = value[, ...])` — добавляет {% if oss == true and backend_name == "YDB" %}[поток изменений (changefeed)](../../../../concepts/cdc.md){% else %}поток изменений{% endif %} с указанным именем и параметрами.

## Параметры потока изменений {#changefeed-options}

* `MODE` — режим работы. Указывает, что именно будет записано в поток при каждом изменении данных в таблице:
    * `KEYS_ONLY` — будут записаны только компоненты первичного ключа и признак изменения.
    * `UPDATES` — будут записаны значения изменившихся столбцов, получившиеся в результате изменения.
    * `NEW_IMAGE` — будут записаны значения всех столбцов, получившиеся в результате изменения.
    * `OLD_IMAGE` — будут записаны значения всех столбцов, предшествующие изменению.
    * `NEW_AND_OLD_IMAGES` - комбинация режимов `NEW_IMAGE` и `OLD_IMAGE`. Будут записаны значения всех столбцов _до_ и _в результате_ изменения.
* `FORMAT` — формат данных, в котором будут записаны данные:
    * `JSON` — записывать данные в формате {% if oss == true and backend_name == "YDB" %}[JSON](../../../../concepts/cdc.md#json-record-structure){% else %}JSON{% endif %}.

    {% if audience == "tech" %}

    * `DYNAMODB_STREAMS_JSON` — записывать данные в {% if oss == true and backend_name == "YDB" %}[JSON-формате, совместимом с Amazon DynamoDB Streams](../../../../concepts/cdc.md#dynamodb-streams-json-record-structure){% else %}JSON-формате, совместимом с Amazon DynamoDB Streams{% endif %}.

    {% endif %}

    * `DEBEZIUM_JSON` — записывать данные в {% if oss == true and backend_name == "YDB" %}[JSON-формате, аналогичном Debezium формату](../../../../concepts/cdc.md#debezium-json-record-structure){% else %}JSON-формате, аналогичном Debezium формату{% endif %}.
* `VIRTUAL_TIMESTAMPS` — включение-выключение {% if oss == true and backend_name == "YDB" %}[виртуальных меток времени](../../../../concepts/cdc.md#virtual-timestamps){% else %}виртуальных меток времени{% endif %}.
* `BARRIERS_INTERVAL` — периодичность выгрузки [барьеров](../../../../concepts/cdc.md#barriers). Тип значения — `Interval`. По умолчанию выключено.
* `RETENTION_PERIOD` — {% if oss == true and backend_name == "YDB" %}[время хранения записей](../../../../concepts/cdc.md#retention-period){% else %}время хранения записей{% endif %}. Тип значения — `Interval`, значение по умолчанию — 24 часа (`Interval('PT24H')`).
* `TOPIC_AUTO_PARTITIONING` — {% if oss == true and backend_name == "YDB" %}[режим автопартиционирования топика](../../../../concepts/cdc.md#topic-partitions){% else %}режим автопартиционирования топика{% endif %}:
    * `ENABLED` — для потока изменений будет создан {% if oss == true and backend_name == "YDB" %}[автопартиционированный топик](../../../../concepts/topic.md#autopartitioning){% else %}автопартиционированный топик{% endif %}. Количество партиций в таком топике увеличивается автоматически по мере роста скорости обновления таблицы. Параметры автопартиционирования топика можно {% if oss == true and backend_name == "YDB" %}[настроить](../alter-topic.md#alter-topic){% else %}настроить{% endif %}.
    * `DISABLED` — для потока изменений будет создан топик без {% if oss == true and backend_name == "YDB" %}[автопартиционирования](../../../../concepts/topic.md#autopartitioning){% else %}автопартиционирования{% endif %}. Это значение по умолчанию.
* `TOPIC_MIN_ACTIVE_PARTITIONS` — {% if oss == true and backend_name == "YDB" %}[количество партиций топика](../../../../concepts/cdc.md#topic-partitions){% else %}количество партиций топика{% endif %}. По умолчанию количество партиций топика равно количеству партиций таблицы. Для автопартиционированных топиков количество партиций увеличивается по мере роста скорости обновления таблицы. Если при создании ченджфида опция `TOPIC_AUTO_PARTITIONING` была отключена (`DISABLED`), то число партиций в топике, связанном с таким ченджфидом, впоследствии изменить нельзя.
* `INITIAL_SCAN` — включение-выключение {% if oss == true and backend_name == "YDB" %}[первоначального сканирования](../../../../concepts/cdc.md#initial-scan){% else %}первоначального сканирования{% endif %} таблицы. По умолчанию выключено.

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

Пример создания потока изменений с виртуальными метками времени и барьерами раз в 10 секунд:

```yql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    VIRTUAL_TIMESTAMPS = TRUE,
    BARRIERS_INTERVAL = Interval('PT10S')
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

Пример создания потока изменений с автопартиционированием:

```yql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    TOPIC_AUTO_PARTITIONING = 'ENABLED',
    TOPIC_MIN_ACTIVE_PARTITIONS = 2
);
```

`DROP CHANGEFEED` — удаляет поток изменений с указанным именем. Приведенный ниже код удалит changefeed с именем `updates_feed`:

```yql
ALTER TABLE `series` DROP CHANGEFEED `updates_feed`;
```
