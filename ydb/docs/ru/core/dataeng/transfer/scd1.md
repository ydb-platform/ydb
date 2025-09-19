
# Реализация SCD1 с использованием трансфера из CDC-источников {{ ydb-full-name }}

## Особенности SCD1

[Медленно меняющиеся измерения (тип 1) или SCD1 (Type 1)](https://ru.wikipedia.org/wiki/Медленно_меняющееся_измерение#Тип_1) — это подход, при котором при изменении атрибута измерения старое значение заменяется новым. Хранится только текущее состояние данных. Этот подход используется, когда:

- историческая информация не требуется;
- важно иметь только актуальные данные;
- необходимо минимизировать размер хранилища данных;
- требуется простая структура данных для аналитики.

Если требования иные и историческая информация нужна, стоит рассмотреть возможность использование [типа 2 медленно меняющихся измерений](scd2.md).

## Реализация SCD1 c помощью трансфера {#scd1}

Для поставки данных в SCD1 таблицу в данной статье будет использоваться следующая комбинация из доступной в {{ ydb-short-name }} функциональности:

1. Таблица-источник будет [строковой](../../concepts/datamodel/table.md#row-oriented-table) для оперативных транзакционных изменений.
2. Таблица-приёмник будет [колоночной](../../concepts/datamodel/table.md#column-oriented-table) для эффективного выполнения аналитических запросов.
3. Подписка на изменения в таблице-источники будет осуществляться через механизм [Change Data Capture (CDC)](../../concepts/cdc.md)
4. За буферизацию изменений будет отвечать неявно создаваемый под CDC [топик](../../concepts/topic.md).
5. За автоматическое перекладывание данных из CDC-топика в таблицу-приёмник будет отвечать [трансфер](../../concepts/transfer.md).

  {% note warning %}

  Transfer не может удалять строки из таблиц, а может выполнять только [`UPSERT`](../../yql/reference/syntax/upsert_into.md) в таблицу-приёмник.
  Поэтому с помощью Transfer можно реализовать только SCD1-soft: удаление помечается флагом и сохраняется момент удаления. Если нужна физическая очистка данных, её можно выполнять через явную команду [`DELETE`](#howtodelete).

  {% endnote %}

  1. Создайте таблицу-источник данных, которая будет генерировать CDC-события:

  ```sql
  -- Создание таблицы-источника
  CREATE TABLE source_customers (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      PRIMARY KEY (id)
  );

  ALTER TABLE `source_customers` ADD CHANGEFEED `updates` WITH (
    FORMAT = 'DEBEZIUM_JSON',
    MODE = 'NEW_AND_OLD_IMAGES'
  );
  ```

  2. Создайте таблицу-приемник данных в формате SCD1:

  ```sql
  CREATE TABLE dimension_scd1 (
      id Utf8 NOT NULL,
      attribute1 Utf8,
      attribute2 Utf8,
      is_deleted Uint8, -- Колоночные таблицы YDB не поддерживают тип Bool в данный момент
      last_update_timestamp Timestamp,
      PRIMARY KEY (id)
  )
  PARTITION BY HASH(id)
  WITH (
    STORE=COLUMN
  )
  ```

  3. Создайте lambda-функцию для обработки CDC-данных и трансфер. Формат Debezium передаёт данные в структуре с полями `before` и `after`.

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

  Примечания:

  - Колонка `id` в принимающей таблице `dimension_scd1` объявлена как `Utf8 NOT NULL`, при этом в Json CDC могут быт переданы данные, которые невозможно привести к строке, то есть результатом конвертации данных из Json может быть значение `NULL`. Функция [`Unwrap`](../../yql/reference/builtins/basic.md#unwrap) гарантирует, что после ее выполнения не может быть значения `NULL` или будет ошибка времени выполнения. Это позволяет гарантировать, что результатом выполнения lambda-функции или будет полностью корректная структура данных, или будет ошибка времени выполнения.

  ## Пример таблицы-источника и работы с CDC

  Для демонстрации работы с данными CDC запишем данные в таблицу-источник в {{ ydb-short-name }}, которая будет генерировать CDC-события:

  1. Вставка новой записи (создаст событие с `op = "c"`):

  ```sql
  -- Вставка новой записи
  INSERT INTO source_customers (id, attribute1, attribute2)
  VALUES ('CUSTOMER_1001', 'John Doe', 'New York');
  ```

  Это действие создаст CDC-событие примерно следующего вида:

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

  2. Обновление существующей записи (создаст событие с `op = "u"`):

  ```sql
  -- Обновление существующей записи
  UPSERT INTO source_customers (id, attribute1, attribute2)
  VALUES ('CUSTOMER_1001', 'John Doe', 'Los Angeles');
  ```

  Это действие создаст CDC-событие примерно следующего вида:

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


  3. Удаление записи (создаст событие с `op = "d"`):

  ```sql
  -- Удаление записи
  DELETE FROM source_customers WHERE id = 'CUSTOMER_1001';
  ```

  Это действие создаст CDC-событие примерно следующего вида:

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
