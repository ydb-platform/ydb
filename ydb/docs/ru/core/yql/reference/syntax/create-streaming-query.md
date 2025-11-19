# CREATE STREAMING QUERY

<!-- markdownlint-disable proper-names -->

Вызов `CREATE STREAMING QUERY` создает [стриминговый запрос](../../../concepts/streaming_query/index.md).

```sql
CREATE [OR REPLACE] STREAMING QUERY [IF NOT EXISTS] <query name> [WITH (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] AS
DO BEGIN
    <query statement1>;
    <query statement2>;
    ...
END DO;
```

Настройки WITH:

- `RUN = (TRUE|FALSE)` - запустить запрос после создания, по умолчанию TRUE

## Указание формата и схемы

Для указания формата и схемы данных используется секция [WITH](select/with.md):

- `FORMAT = (raw|json_each_row)` - см. описание ниже,
- `SCHEMA (...)` —  описание схемы хранимых данных.

### Формат json_each_row {#json_each_row}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате внутри каждого сообщения в топике должен находиться объект в корректном JSON-представлении. Такой формат используется при передаче данных через потоковые системы, например, Apache Kafka или [Топики {{ydb-full-name}}](../../../concepts/datamodel/topic.md).
Несколько отдельных json в одном сообщении не поддерживается; JSON-список также не поддерживается.

### Формат raw {#raw}

Данный формат позволяет считывать содержимое сообщений как есть, в "сыром" виде. Считанные таким образом данные можно обработать средствами [YQL](../../../yql/reference/udf/list/string). Cхема по умолчанию: `SCHEMA(Data String)`.

### Поддерживаемые типы данных {#schema}

Таблица всех поддерживаемых типов  в схеме запроса:
|Тип                                  |json_each_row|raw|
|-------------------------------------|-------------|---|
|`Int8`, `Int16`, `Int32`, `Int64`,<br/>`Uint8`, `Uint16`, `Uint32`, `Uint64`,<br/>`Float`, `Double`|✓||
|`Bool`                               |✓            |   |
|`DyNumber`                           |             |   |
|`String`, `Utf8`                     |✓            |✓  |
|`Json`                               |             |✓  |
|`JsonDocument`                       |             |   |
|`Yson`                               |             |   |
|`Uuid`                               |✓            |   |
|`Date`, `Datetime`, `Timestamp`,<br/>`TzDate`, `TzDateTime`, `TzTimestamp`|    |  |
|`Interval`                           |            |   |
|`Date32`, `Datetime64`, `Timestamp64`,<br/>`Interval64`,<br/>`TzDate32`, `TzDateTime64`, `TzTimestamp64`|     |    |
|`Optional<T>`                        |✓            |✓  |


### Примеры

Чтение в формате `json_each_row`:

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
    PRAGMA pq.Consumer = "ConsumerName";
    $input = SELECT * FROM `source_name`.`input_topic_name`
    WITH (
        FORMAT = "json_each_row",
        SCHEMA (
            time String NOT NULL,
            level String NOT NULL,
            host String NOT NULL));
    $filtered = SELECT * FROM $input WHERE level = "error";

    $number_errors =
        SELECT COUNT(*) AS error_count, CAST(HOP_START() as String) as ts FROM $filtered
        GROUP BY HoppingWindow(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'), host;
    
    $json = SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) FROM $number_errors;
    INSERT INTO `source_name`.`output_topic_name` SELECT * FROM $json;
END DO;
```

Чтение в формате `raw`:

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
    $input = SELECT CAST(data AS Json) AS json FROM `source_name`.`input_topic_name`
    WITH (
        FORMAT="raw",
        SCHEMA=(data String));
    $parsed = SELECT JSON_VALUE(json, "$.field1") as field1, JSON_VALUE(json, "$.field2") as field2 FROM $input;
    INSERT INTO `source_name`.`output_topic_name` SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) FROM $parsed;
END DO;
```
