
# Реализация SCD1 с использованием трансфера из CDC-источников {{ ydb-full-name }}

## Особенности SCD1

SCD1 (Type 1) — это подход, при котором при изменении атрибута измерения старое значение заменяется новым. Хранится только текущее состояние данных. Этот подход используется, когда:


- историческая информация не требуется;
- важно иметь только актуальные данные;
- необходимо минимизировать размер хранилища данных;
- требуется простая структура данных для аналитики.

## Реализация SCD1 c помощью трансфера {#scd1}

  {% note warning %}

  Transfer не может удалять строки из таблиц, а может выполнять только `UPSERT` в таблицу-приёмник.
  Поэтому с помощью Transfer можно реализовать только SCD1-soft: удаление помечается флагом и сохраняется момент удаления. Если нужна физическая очистка данных, её можно выполнять или через явную команду `DELETE` или с помощью TTL.

  {% endnote %}

  1. Создайте таблицу:

  ```sql
    CREATE TABLE dimension_scd1 (
        id Utf8 NOT NULL,
        attribute1 Utf8,
        attribute2 Utf8,
        is_deleted Uint8, --Колоночные таблицы YDB не поддерживают тип Bool в данный момент
        last_update_timestamp Timestamp,
        PRIMARY KEY (id)
    )
    PARTITION BY HASH(id)
    WITH(STORE=COLUMN)
  ```

  2. Создайте lambda-функцию для обработки CDC-данных и трансфер. Формат Debezium передаёт данные в структуре с полями `before` и `after`:

  ```sql
    $transformation_lambda = ($msg) -> {
        $cdc_data = CAST($msg._data AS JSON);

        -- Определяем тип операции
        $operation = Yson::ConvertToString($cdc_data.payload.op);
        $is_deleted = IF($operation == "d", TRUE, FALSE);

        -- Получаем данные в зависимости от типа операции
        $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

        -- Если данные не пришли (выполнена команда DELETE на несуществующий ключ, то проигнорируем запись)
        return IF($data is not NULL,[
            <|
                id: Unwrap(CAST(Yson::ConvertToString($data.id) AS Utf8)),
                attribute1: CAST(Yson::ConvertToString($data.attribute1) AS Utf8),
                attribute2: CAST(Yson::ConvertToString($data.attribute2) AS Utf8),
                is_deleted: IF($is_deleted, 1ut, 0ut),
                last_update_timestamp: DateTime::FromMilliseconds(Yson::ConvertToUint64($cdc_data.payload.source.ts_ms))
          |>
      ], []);
  };

  -- В данном примере настраивается высокая частота обновлений таблицы-приемника.
  -- Это делается исключительно для наглядности. Для production-сценариев стоит настраивать большие значения
  CREATE TRANSFER dimension_scd1_cdc_transfer
        FROM `source_customers/updates` TO dimension_scd1 USING $transformation_lambda
        with(FLUSH_INTERVAL=Interval("PT1S"));
  ```

  Примечания:
  - Debezium передает данные в формате Json. Для работы с типом Json в YQL используется поддиалект Yson, поэтому для конвертации Json-типов данных используются функции `Yson::ConvertToXXX()`.
  - Колонка `id` в принимающей таблице `dimension_scd1` объявлена как `Utf8 NOT NULL`, при этом в Json CDC могут быт переданы данные, которые невозможно привести к строке, то есть результатом конвертации данных из Json может быть значение `NULL`. Функция [`Unwrap`](../../yql/reference/builtins/basic.md#unwrap) гарантирует, что после ее выполнения не может быть значения `NULL` или будет ошибка времени выполнения. Это позволяет гарантировать, что результатом выполнения lambda-функции или будет полностью корректная структура данных, или будет ошибка времени выполнения.

  ## Пример таблицы-источника и работы с CDC

  Для демонстрации работы с CDC данными, создадим таблицу-источник в YDB, которая будет генерировать CDC-события:

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

  Теперь выполним операции с данными, которые будут генерировать CDC-события:

  **1. Вставка новой записи (создаст событие с `op = "c"`):**

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

  **2. Обновление существующей записи (создаст событие с `op = "u"`):**

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

  **3. Удаление записи (создаст событие с `op = "d"`):**

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

  Эти CDC-события будут записаны в топик YDB, откуда их будет читать трансфер и преобразовывать в записи таблицы `dimension_scd1` с помощью lambda-функции, описанной выше. Обратите внимание, что:

  * При создании записи (`op = "c"`) данные берутся из поля `after`
  * При обновлении записи (`op = "u"`) данные также берутся из поля `after`
  * При удалении записи (`op = "d"`) данные берутся из поля `before`, а флаг `is_deleted` устанавливается в `true`

  Особенности обработки CDC данных в формате Debezium:

  * Формат сообщения содержит поля `payload.before` и `payload.after` с состоянием до и после изменения
  * Поле `payload.op` указывает тип операции: "c" (create), "u" (update), "d" (delete)
  * Поле `payload.ts_ms` содержит временную метку события в миллисекундах
  * При удалении (`op = "d"`) данные находятся в поле `before`, а при создании/обновлении - в поле `after`. Если выполняется удаление несуществующей строки, то будет сформировано сообщение с типом операции `op = "d"`, но с пустыми полями `before` и `after`

