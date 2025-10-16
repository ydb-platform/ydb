# Использование механизма TRANSFER для реализации SCD2 на основе данных из CDC-источников в {{ ydb-full-name }}

В этой статье описывается реализация паттерна Slowly Changing Dimensions Type 2 (SCD2) в {{ ydb-short-name }}.

## Используемые инструменты

Для поставки данных в SCD2 таблицу в данной статье будет использоваться следующая комбинация из доступной в {{ ydb-short-name }} функциональности:

1. Таблица-источник будет [строковой](../../../concepts/datamodel/table.md#row-oriented-table) для оперативных транзакционных изменений;
2. Таблица-приёмник будет [колоночной](../../../concepts/datamodel/table.md#column-oriented-table) для эффективного выполнения аналитических запросов;
3. Подписка на изменения в таблице-источнике будет осуществляться через механизм [Change Data Capture (CDC)](../../../concepts/cdc.md);
4. За буферизацию изменений будет отвечать неявно создаваемый для CDC [топик](../../../concepts/datamodel/topic.md);
5. За автоматическое перекладывание данных из CDC-топика в таблицу-приёмник будет отвечать [трансфер](../../../concepts/transfer.md).

## Создайте таблицу-источник данных, которая будет генерировать CDC-события

```sql
CREATE TABLE source_customers (
    id Utf8 NOT NULL,
    attribute1 Utf8,
    attribute2 Utf8,
    change_time Timestamp,
    PRIMARY KEY (id)
);

ALTER TABLE `source_customers` ADD CHANGEFEED `updates` WITH (
  FORMAT = 'DEBEZIUM_JSON',
  MODE = 'NEW_AND_OLD_IMAGES'
);
```

## Создайте таблицу-приемник данных в формате SCD2

  ```sql
    CREATE TABLE dimension_scd2 (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      valid_from Timestamp NOT NULL,
      deleted_at Timestamp ,
      PRIMARY KEY (valid_from, id)
    )
    PARTITION BY HASH(valid_from, id)
    WITH (
      STORE=COLUMN
    )
  ```

## Создайте трансфер и lambda-функцию для обработки CDC-данных

  Особенности обработки CDC данных в формате Debezium:

  * При создании записи (`op = "c"`) данные берутся из поля `after`, в поле `valid_from` сохраняется значение момента времени изменения записи из поля `changed_at`.
  * При обновлении записи (`op = "u"`) данные также берутся из поля `after`, в поле `valid_from` сохраняется значение момента времени изменения записи из поля `changed_at`.
  * При удалении записи (`op = "d"`) данные берутся из поля `before`, а в поле `deleted_at` устанавливается системный момент времени получения информации про удаление записи.

  ```sql
  $transformation_lambda = ($msg) -> {
  $cdc_data = CAST($msg._data AS Json);

  -- Определяем тип операции
  $operation = Json::ConvertToString($cdc_data.payload.op);
  $is_deleted = $operation == "d";

  -- Получаем данные в зависимости от типа операции
  $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

  $result =  IF($data is not NULL, <|
          id: Unwrap(CAST(Json::ConvertToString($data.id) AS Utf8)),
          attribute1: CAST(Json::ConvertToString($data.attribute1) AS Utf8),
          attribute2: CAST(Json::ConvertToString($data.attribute2) AS Utf8),
          deleted_at: IF($is_deleted, DateTime::FromMilliseconds(Json::ConvertToUint64($cdc_data.payload.source.ts_ms)), NULL),
          valid_from : Unwrap(cast(datetime::MakeDatetime(datetime::ParseIso8601(Json::ConvertToString($data.change_time))) as timestamp)),
      |>, NULL);

  return $result;
  };

  -- В данном примере настраивается высокая частота обновлений таблицы-приемника.
  -- Это делается исключительно для наглядности. Для production-сценариев стоит настраивать большие значения
    CREATE TRANSFER dimension_scd2_cdc_transfer
      FROM `source_customers/updates` TO dimension_scd2 USING $transformation_lambda
      WITH (
        FLUSH_INTERVAL=Interval("PT1S")
      );
  ```

  {% note info %}

  Колонка `id` в принимающей таблице `dimension_scd2` объявлена как `Utf8 NOT NULL`, при этом в Json CDC могут быт переданы данные, которые невозможно привести к строке, то есть результатом конвертации данных из Json может быть значение `NULL`. Функция [`Unwrap`](../../../yql/reference/builtins/basic.md#unwrap) гарантирует, что после ее выполнения не может быть значения `NULL` или будет ошибка времени выполнения. Это позволяет гарантировать, что результатом выполнения lambda-функции или будет полностью корректная структура данных, или будет ошибка времени выполнения.

  {% endnote %}

  ## Демонстрация работы

  Для демонстрации работы с данными CDC запишем данные в таблицу-источник в {{ ydb-short-name }}, которая будет генерировать CDC-события:

  ### Вставка новой записи

  ```sql
  INSERT INTO source_customers (id, attribute1, attribute2, change_time)
  VALUES ('CUSTOMER_1001', 'John Doe', 'Los Angeles', CurrentUtcTimestamp());
  ```

  Это действие создаст CDC-событие с типом операции `op = "c"` примерно следующего вида:

  ```json
  {
    "payload":{
      "op":"c",
      "source":{
        "txId":18446744073709551615,
        "connector":"ydb",
        "version":"1.0.0",
        "step":1755883683690,
        "ts_ms":1755883683652,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "change_time":"2025-08-22T17:28:03.648313Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  В результате исполнения команды выше в таблице `dimension_scd2` будет следующее содержимое:

  | id             | attribute1 | attribute2    | deleted\_at                 | valid_from                  |
  | -------------- | ---------- | ------------- | --------------------------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | NULL                        | 2025-08-22T17:28:03.648313Z |

  ### Обновление новой записи

  ```sql
  UPSERT INTO source_customers (id, attribute1, attribute2, change_time)
  VALUES ('CUSTOMER_1001', 'John Doe 2', 'Los Angeles 2', CurrentUtcTimestamp());
  ```

  Это действие создаст CDC-событие с типом операции `op = "u"` примерно следующего вида:

  ```json
  {
    "payload":{
      "op":"u",
      "source":{
        "txId":18446744073709551615,
        "connector":"ydb",
        "version":"1.0.0",
        "step":1755883278000,
        "ts_ms":1755883278357,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe 2",
        "change_time":"2025-08-22T17:31:18.357503Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles 2"
      },
      "before":{
        "attribute1":"John Doe 2",
        "change_time":"2025-08-22T17:28:03.648313Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles 2"
      }
    }
  }
  ```

  В результате исполнения команды выше в таблице `dimension_scd2` будет следующее содержимое:

  | id             | attribute1 | attribute2    | deleted\_at                 | valid_from                  |
  | -------------- | ---------- | ------------- | --------------------------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | NULL                        | 2025-08-22T17:28:03.648313Z |
  | CUSTOMER\_1001 | John Doe 2 | New York 2    | NULL                        | 2025-08-22T17:31:18.357503Z |

  ### Удаление записи

  ```sql
  DELETE FROM source_customers WHERE id = 'CUSTOMER_1001';
  ```

  Это действие создаст CDC-событие с типом операции `op = "d"` примерно следующего вида:

  ```json
  {
    "payload":{
      "op":"d",
      "source":{
        "txId":18446744073709551615,
        "connector":"ydb",
        "version":"1.0.0",
        "step":1755883827000,
        "ts_ms":1755883827948,
        "snapshot":false
      },
      "before":{
        "attribute1":"John Doe",
        "change_time":"2025-08-22T17:38:03.648313Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  В результате исполнения команды выше в таблице `dimension_scd2` будет следующее содержимое:

  | id             | attribute1 | attribute2    | deleted\_at                 | valid_from                  |
  | -------------- | ---------- | ------------- | --------------------------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | NULL                        | 2025-08-22T17:28:03.648313Z |
  | CUSTOMER\_1001 | John Doe 2 | New York 2    | NULL                        | 2025-08-22T17:31:18.357503Z |
  | CUSTOMER\_1001 | John Doe 2 | New York 2    | 2025-08-22T17:38:03.648313Z | 2025-08-22T17:38:03.648313Z |

  ### Пример запроса для получения актуальных данных

  Для получения данных из SCD2 append only таблиц можно использовать следующий запрос, который получает данные на момент времени `2025-08-22 19:11:30`:

  ```sql
  DECLARE $as_of AS Timestamp;
  $as_of = Timestamp("2025-08-22T19:11:30.000000Z");

  SELECT
    id,
    data.attribute1 AS attribute1,
    data.attribute2 AS attribute2,
    data.valid_from AS valid_from,
    data.deleted_at AS deleted_at
  FROM (
    SELECT
      id,
      MAX_BY(TableRow(), valid_from) AS data
    FROM dimension_scd2
    WHERE
        valid_from <= $as_of
        AND (deleted_at IS NULL OR deleted_at > $as_of)  -- запись ещё не удалена к моменту X
    GROUP BY id
  )
  ```
