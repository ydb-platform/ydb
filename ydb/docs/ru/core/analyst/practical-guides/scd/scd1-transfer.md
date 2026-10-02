# Использование механизма TRANSFER для реализации SCD1 на основе данных из CDC-источников в {{ ydb-full-name }}

В этой статье описывается реализация паттерна Slowly Changing Dimensions Type 1 (SCD1) в {{ ydb-short-name }}.

## Используемые инструменты

Для поставки данных в SCD1-таблицу в данной статье будет использоваться следующая комбинация функциональных возможностей {{ ydb-short-name }}:

1. Таблица-источник будет [строковой](../../../concepts/datamodel/table.md#row-oriented-table) для оперативных транзакционных изменений;
2. Таблица-приёмник будет [колоночной](../../../concepts/datamodel/table.md#column-oriented-table) для эффективного выполнения аналитических запросов;
3. Подписка на изменения в таблице-источнике будет осуществляться через механизм [Change Data Capture (CDC)](../../../concepts/cdc.md);
4. За буферизацию изменений будет отвечать неявно создаваемый под CDC [топик](../../../concepts/datamodel/topic.md);
5. За автоматическое перекладывание данных из CDC-топика в таблицу-приёмник будет отвечать [трансфер](../../../concepts/transfer.md).

{% note warning %}

Transfer не может удалять строки из таблиц, а может выполнять только [`UPSERT`](../../../yql/reference/syntax/upsert_into.md) в таблицу-приёмник.
Поэтому с помощью Transfer можно реализовать только SCD1-soft: удаление помечается флагом и сохраняется момент удаления. Если нужна физическая очистка данных, её можно выполнять через явную команду [`DELETE`](#howtodelete).

{% endnote %}

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

  ## Создайте таблицу-приемник данных в формате SCD1

  ```sql
  CREATE TABLE dimension_scd1 (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      is_deleted Uint8,
      last_update_timestamp Timestamp,
      PRIMARY KEY (id)
  )
  PARTITION BY HASH(id)
  WITH (
    STORE=COLUMN
  )
  ```

  ## Создайте трансфер и lambda-функцию для обработки CDC-данных

  Особенности обработки CDC данных в формате Debezium:

  * Формат сообщения содержит поля `payload.before` и `payload.after` с состоянием до и после изменения.
  * Поле `payload.op` указывает тип операции: "c" (create), "u" (update), "d" (delete).
  * Поле `payload.ts_ms` содержит временную метку события в миллисекундах.
  * При удалении (`op = "d"`) данные находятся в поле `before`, а при создании/обновлении - в поле `after`. Если выполняется удаление несуществующей строки, то будет сформировано сообщение с типом операции `op = "d"`, но с пустыми полями `before` и `after`.

  ```sql
  $transformation_lambda = ($msg) -> {
      $cdc_data = CAST($msg._data AS Json);

      -- Определяем тип операции
      $operation = Json::ConvertToString($cdc_data.payload.op);
      $is_deleted = $operation == "d";

      -- Получаем данные в зависимости от типа операции
      $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

      -- Если данные не пришли (выполнена команда DELETE на несуществующий ключ, то проигнорируем запись)
      return IF($data IS NOT NULL,
          <|
              id: Unwrap(CAST(Json::ConvertToString($data.id) AS Utf8)),
              attribute1: CAST(Json::ConvertToString($data.attribute1) AS Utf8),
              attribute2: CAST(Json::ConvertToString($data.attribute2) AS Utf8),
              is_deleted: CAST($is_deleted AS Uint8),
              last_update_timestamp: DateTime::FromMilliseconds(Json::ConvertToUint64($cdc_data.payload.source.ts_ms))
        |>
      , NULL);
  };

  -- В данном примере настраивается высокая частота обновлений таблицы-приемника.
  -- Это делается исключительно для наглядности. Для production-сценариев стоит настраивать большие значения
  CREATE TRANSFER dimension_scd1_cdc_transfer
    FROM `source_customers/updates` TO dimension_scd1 USING $transformation_lambda
    WITH (
      FLUSH_INTERVAL=Interval("PT1S")
    );
  ```

  {% note info %}

  Колонка `id` в принимающей таблице `dimension_scd1` объявлена как `Utf8 NOT NULL`, при этом в Json CDC могут быт переданы данные, которые невозможно привести к строке, то есть результатом конвертации данных из Json может быть значение `NULL`. Функция [`Unwrap`](../../../yql/reference/builtins/basic.md#unwrap) гарантирует, что после ее выполнения не может быть значения `NULL` или будет ошибка времени выполнения. Это позволяет гарантировать, что результатом выполнения lambda-функции или будет полностью корректная структура данных, или будет ошибка времени выполнения.
  
  {% endnote %}

  ## Демонстрация работы

  Для демонстрации работы с данными CDC запишем данные в таблицу-источник в {{ ydb-short-name }}, которая будет генерировать CDC-события:

  ### Вставка новой записи

  ```sql
  INSERT INTO source_customers (id, attribute1, attribute2)
  VALUES ('CUSTOMER_1001', 'John Doe', 'New York');
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
        "step":1755868513260,
        "ts_ms":1755868513219,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"New York"
      }
    }
  }
  ```

  В результате исполнения команды выше в таблице `dimension_scd1` будет следующее содержимое:

  | id             | attribute1 | attribute2    | is\_deleted | last_update_timestamp       |
  | -------------- | ---------- | ------------- | ----------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | New York      | 0           | 2025-08-22T13:15:13.219000Z |

  ### Обновление существующей записи

  ```sql
  UPSERT INTO source_customers (id, attribute1, attribute2)
  VALUES ('CUSTOMER_1001', 'John Doe', 'Los Angeles');
  ```

  Это действие создаст CDC-событие с типом операции `op = "u" примерно следующего вида:

  ```json
  {
    "payload":{
        "op":"u",
        "source":{
          "txId":18446744073709551615,
          "connector":"ydb",
          "version":"1.0.0",
          "step":1755868795050,
          "ts_ms":1755868795719,
          "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      },
      "before":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"New York"
      }
    }
  }
  ```

  В результате исполнения команды выше в таблице `dimension_scd1` будет следующее содержимое:

  | id             | attribute1 | attribute2    | is\_deleted | last_update_timestamp       |
  | -------------- | ---------- | ------------- | ----------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | Los Angeles   | 0           | 2025-08-22T13:19:55.719000Z |


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
        "step":1755868931000,
        "ts_ms":1755868931109,
        "snapshot":false
      },
      "before":{
        "attribute1":"John Doe",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  В результате исполнения команды выше в таблице `dimension_scd1` будет следующее содержимое:

  | id             | attribute1 | attribute2    | is\_deleted | last_update_timestamp       |
  | -------------- | ---------- | ------------- | ----------- | --------------------------- |
  | CUSTOMER\_1001 | John Doe   | Los Angeles   | 1           | 2025-08-22T13:22:11.109000Z |


## Пример удаления данных из таблицы-приемника {#howtodelete}

Для удаления данных из таблицы-приемника можно использовать явную команду `DELETE`:

```sql
DELETE from dimension_scd1 WHERE is_deleted != 0ut
```