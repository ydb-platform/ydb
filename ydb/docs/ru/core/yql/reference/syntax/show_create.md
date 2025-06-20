# SHOW CREATE

`SHOW CREATE` возвращает запрос, возможно состоящий из нескольких DDL-выражений, необходимых для воссоздания структуры выбранного объекта: {% if oss == true and backend_name == 'YDB' %}[таблицы](../../../concepts/datamodel/table.md){% else %}таблицы{% endif %}{% if feature_view and oss == true and backend_name == 'YDB' %} или [представления](../../../concepts/datamodel/view.md){% endif %}{% if feature_view and backend_name != 'YDB' %} или представления{% endif %}.

## Синтаксис

```yql
SHOW CREATE [TABLE|VIEW] <name>;
```

## Параметры

* `TABLE|VIEW` - тип объекта: `TABLE` для таблицы или `VIEW` для представления.
* `<name>` - имя объекта, также может быть указан абсолютный путь до объекта.

## Результат выполнения

Команда возвращает **ровно одну строку** с тремя колонками:

| Path            | PathType   | CreateQuery                        |
|-----------------|------------|------------------------------------|
| Путь            | Table/View | DDL-выражения для создания объекта |

- **Path** — путь к объекту (например, `MyTable` или `MyView`).
- **PathType** — тип объекта: `Table` или `View`.
- **CreateQuery** — полный набор DDL-выражений, необходимых для создания объекта:
    - Для таблиц: основной оператор [CREATE TABLE](create_table/index.md) (с путем относительно базы), а также дополнительные команды, необходимые для описания текущего состояния и настроек:
        - [ALTER TABLE ... ALTER INDEX](alter_table/secondary_index#alter-index)— для задания настроек партицирования вторичных индексов.
        {% if feature_changefeed and backend_name == "YDB" %}
        - [ALTER TABLE ... ADD CHANGEFEED](alter_table/changefeed.md)— для добавления потока изменений.
        {% endif %}
        {% if feature_serial %}
        - `ALTER SEQUENCE` — для восстановления состояния `Sequence` у колонок типа [Serial](../../../yql/reference/types/serial.md).
        {% endif %}
    {% if feature_view %}
    - Для представлений: определение посредством команды [CREATE VIEW](create-view.md), а также, если необходимо, выражения, которые были зафиксированы представлением из контекста создания, например, [PRAGMA TablePathPrefix](pragma#table-path-prefix).
    {% endif %}

## Примеры

### Строковые таблицы

```yql
SHOW CREATE TABLE my_table;
```

| Path            | PathType  | CreateQuery                    |
|-----------------|-----------|--------------------------------|
| `my_table`      | `Table`   | `CREATE TABLE...` - см. ниже   |

```yql
CREATE TABLE `my_table` (
    `Key1` Uint32 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` Serial4 NOT NULL,
    `Value1` Utf8 FAMILY `my_family`,
    `Value2` Bool,
    `Value3` String,
    INDEX `my_index` GLOBAL SYNC ON (`Key2`, `Value1`, `Value2`),
    FAMILY `my_family` (COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
);

ALTER TABLE `my_table`
    ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT30M'), TOPIC_MIN_ACTIVE_PARTITIONS = 3)
;

ALTER SEQUENCE `/Root/my_table/_serial_column_Key3` START WITH 101 INCREMENT BY 404 RESTART;

ALTER TABLE `my_table`
    ALTER INDEX `my_index` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000)
;
```

### Колоночные таблицы

```yql
SHOW CREATE TABLE my_table;
```

| Path            | PathType  | CreateQuery                    |
|-----------------|-----------|--------------------------------|
| `my_table`      | `Table`   | `CREATE TABLE...` - см. ниже   |

```yql
CREATE TABLE `my_table` (
    `Key1` Uint64 NOT NULL,
    `Key2` Utf8 NOT NULL,
    `Key3` Int32 NOT NULL,
    `Value1` Utf8,
    `Value2` Int16,
    `Value3` String,
    FAMILY `default` (COMPRESSION = 'zstd'),
    FAMILY `Family1` (COMPRESSION = 'off'),
    FAMILY `Family2` (COMPRESSION = 'lz4'),
    PRIMARY KEY (`Key1`, `Key2`, `Key3`)
)
PARTITION BY HASH (`Key1`, `Key2`)
WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
    TTL =
        INTERVAL('PT10S') TO EXTERNAL DATA SOURCE `/Root/tier1`,
        INTERVAL('PT1H') DELETE
    ON Key1 AS SECONDS
);
```

### Представления

```yql
SHOW CREATE VIEW my_view;
```

| Path            | PathType  | CreateQuery                              |
|-----------------|-----------|------------------------------------------|
| `my_view`       | `View`    | `PRAGMA TablePathPrefix...` - см. ниже   |

```yql
PRAGMA TablePathPrefix = '/Root/DirA/DirB/DirC';

CREATE VIEW `my_view` WITH (security_invoker = TRUE) AS
SELECT
    *
FROM
    test_table
;
```

