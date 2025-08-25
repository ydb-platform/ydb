
# Реализация SCD2 (append-only) с использованием трансфера

## Особенности SCD2 и подход append-only

SCD2 (Type 2) — это подход, при котором при изменении атрибута измерения создается новая запись, а старая помечается как неактуальная. Таким образом, сохраняется история изменений. Этот подход используется, когда:
- Требуется отслеживать историю изменений данных
- Необходимо выполнять анализ данных с учетом временных периодов
- Важно сохранять аудиторский след изменений
- Требуется возможность восстановления состояния данных на определенный момент времени

## Реализация SCD2 с помощью трансфера {#scd2}

  {% note warning %}

  Transfer не может удалять строки из таблиц, а может выполнять только UPSERT в таблицу-приёмник.
  Поэтому с помощью Transfer можно реализовать только SCD2 append only: удаление помечается флагом и сохраняется момент удаления. Если нужна физическая очистка данных, ее можно выполнять или через явную команду DELETE или с помощью TTL.

  {% endnote %}

{% list tabs %}

- SCD2 append only

  1. Создайте таблицу:

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
    WITH(STORE=COLUMN)
  ```

  2. Создайте lambda-функцию для обработки CDC данных и трансфер

  ```sql
  $transformation_lambda = ($msg) -> {
        $cdc_data = CAST($msg._data AS JSON);

        -- Определяем тип операции
        $operation = Yson::ConvertToString($cdc_data.payload.op);
        $is_deleted = IF($operation == "d", TRUE, FALSE);

        -- Получаем данные в зависимости от типа операции
        $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

        $result =  IF($data is not NULL, <|
                id: Unwrap(CAST(Yson::ConvertToString($data.id) AS Utf8)),
                attribute1: CAST(Yson::ConvertToString($data.attribute1) AS Utf8),
                attribute2: CAST(Yson::ConvertToString($data.attribute2) AS Utf8),
                deleted_at: IF($is_deleted, DateTime::FromMilliseconds(Yson::ConvertToUint64($cdc_data.payload.source.ts_ms)), NULL),
                valid_from : Unwrap(cast(datetime::MakeDatetime(datetime::ParseIso8601(Yson::ConvertToString($data.change_time))) as timestamp)),
            |>, NULL);

        return $result;
    };

  -- В данном примере настраивается высокая частота обновлений таблицы-приемника.
  -- Это делается исключительно для наглядности. Для production-сценариев стоит настраивать большие значения
    CREATE TRANSFER dimension_scd2_cdc_transfer
          FROM `source_customers2/updates` TO dimension_scd2 USING $transformation_lambda
          with(FLUSH_INTERVAL=Interval("PT1S"));
  ```

  Примечания:
  - Debezium передает данные в формате Json. Для работы с типом Json в YQL используется поддиалект Yson, поэтому для конвертации Json-типов данных используются функции `Yson::ConvertToXXX()`.
  - Колонка `id` в принимающей таблице `dimension_scd2` объявлена как `Utf8 NOT NULL`, при этом в Json CDC могут быт переданы данные, которые невозможно привести к строке, то есть результатом конвертации данных из Json может быть значение `NULL`. Функция [`Unwrap`](../../yql/reference/builtins/basic.md#unwrap) гарантирует, что после ее выполнения не может быть значения `NULL` или будет ошибка времени выполнения. Это позволяет гарантировать, что результатом выполнения lambda-функции или будет полностью корректная структура данных, или будет ошибка времени выполнения.

  ## Пример таблицы-источника и работы с CDC

  Для демонстрации работы с CDC данными, создадим таблицу-источник в YDB, которая будет генерировать CDC-события:

  ```sql
  -- Создание таблицы-источника
    CREATE TABLE source_customers2 (
        id Utf8 NOT NULL,
        attribute1 Utf8,
        attribute2 Utf8,
        change_time Timestamp,
        PRIMARY KEY (id)
    );

    ALTER TABLE `source_customers2` ADD CHANGEFEED `updates` WITH (
    FORMAT = 'DEBEZIUM_JSON',
    MODE = 'NEW_AND_OLD_IMAGES'
    );
  ```

  Теперь выполним операции с данными, которые будут генерировать CDC-события:

  **1. Вставка или обновление новой записи (создаст событие с `op = "c"`):**

  ```sql
  -- Вставка новой записи
  INSERT INTO source_customers2 (id, attribute1, attribute2, change_time)
  VALUES ('CUSTOMER_1002', 'John Doe', 'Los Angeles', CurrentUtcTimestamp());
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
        "step":1755883683690,
        "ts_ms":1755883683652,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe",
        "change_time":"2025-08-22T17:28:03.648313Z",
        "id":"CUSTOMER_1002",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  **1. Вставка или обновление новой записи (создаст событие с `op = "с"` или `op = "u"`):**

  ```sql
  -- Вставка новой записи
  INSERT INTO source_customers2 (id, attribute1, attribute2, change_time)
  VALUES ('CUSTOMER_1001', 'John Doe 2', 'Los Angeles 2', CurrentUtcTimestamp());
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
        "step":1755883278000,
        "ts_ms":1755883278357,
        "snapshot":false
      },
      "after":{
        "attribute1":"John Doe 2",
        "change_time":"2025-08-22T17:21:18.357503Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles 2"
      },
      "before":{
        "attribute1":"John Doe 2",
        "change_time":"2025-08-22T17:12:51.502806Z",
        "id":"CUSTOMER_1001",
        "attribute2":"Los Angeles 2"
      }
    }
  }
  ```

  **3. Удаление записи (создаст событие с `op = "d"`):**

  ```sql
  -- Удаление записи
  DELETE FROM source_customers2 WHERE id = 'CUSTOMER_1002';
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
        "step":1755883827000,
        "ts_ms":1755883827948,
        "snapshot":false
      },
      "before":{
        "attribute1":"John Doe",
        "change_time":"2025-08-22T17:28:03.648313Z",
        "id":"CUSTOMER_1002",
        "attribute2":"Los Angeles"
      }
    }
  }
  ```

  Эти CDC-события будут записаны в топик YDB, откуда их будет читать трансфер и преобразовывать в записи таблицы `dimension_scd2` с помощью lambda-функции, описанной выше. Обратите внимание, что:

  * При создании записи (`op = "c"`) данные берутся из поля `after`, в поле `valid_from` сохраняется значение момента времени изменения записи из поля `changed_at`.
  * При обновлении записи (`op = "u"`) данные также берутся из поля `after`, в поле `valid_from` сохраняется значение момента времени изменения записи из поля `changed_at`.
  * При удалении записи (`op = "d"`) данные берутся из поля `before`, а в поле `deleted_at` устанавливается системный момент времени получения информации про удаление записи.

  ## Пример запроса для получения актуальных данных
  

  Для получения данных из SCD2 append only структур хранения можно использовать следующий запрос, который получает данные на момент времени `2025-08-22 19:11:30`:

  ```sql
  DECLARE $as_of AS Timestamp;
  $as_of = Datetime::MakeDatetime(DateTime::Parse("%Y-%m-%d %H:%M:%S")("2025-08-22 19:11:30"));

  SELECT
    id,
    attribute1,
    attribute2,
    valid_from,
    deleted_at
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY valid_from DESC) AS rn
    FROM dimension_scd2 AS t
    WHERE
      t.valid_from <= $as_of
      AND (t.deleted_at IS NULL OR t.deleted_at > $as_of)  -- запись ещё не удалена к моменту X
  )
  WHERE rn = 1;
  ```

- SCD2 с помощью таблицы хранения промежуточных данных и операции слияния

  Для работы с CDC данными {{ ydb-short-name }} в формате Debezium при реализации SCD2:

  1. Создайте таблицу для приема изменений:

  ```sql
    CREATE TABLE dimension_scd_changes (
        id Utf8 NOT NULL,
        attribute1 Utf8,
        attribute2 Utf8,
        change_time Timestamp NOT NULL,
        operation Utf8,
        row_operation_at Timestamp ,
        PRIMARY KEY (change_time, id)
    )
    PARTITION BY HASH(change_time, id)
    WITH(STORE=COLUMN)
  ```

  1. Создайте таблицу для хранения данных:

  ```sql
    CREATE TABLE dimension_scd2_final (
        id Utf8 NOT NULL,
        attribute1 Utf8,
        attribute2 Utf8,
        valid_from Timestamp NOT NULL,
        valid_to Timestamp,
        is_current Timestamp ,
        is_deleted Uint8,
        PRIMARY KEY (valid_from, id)
    )
    PARTITION BY HASH(valid_from, id)
    WITH(STORE=COLUMN)
  ```

  1. Создайте трансфер для переноса данных:

  ```sql
  $transformation_lambda = ($msg) -> {
    $cdc_data = CAST($msg._data AS JSON);

    -- Определяем тип операции
    $operation = Yson::ConvertToString($cdc_data.payload.op);
    $is_deleted = IF($operation == "d", TRUE, FALSE);

    $operation_type = case $operation
                            WHEN 'd' THEN "DELETE"
                            WHEN 'u' THEN "UPDATE"
                            WHEN 'c' THEN "CREATE"
                            else NULL
                        END;

    -- Получаем данные в зависимости от типа операции
    $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

    $result =  IF($data is not NULL, <|
            id: Unwrap(CAST(Yson::ConvertToString($data.id) AS Utf8)),
            attribute1: CAST(Yson::ConvertToString($data.attribute1) AS Utf8),
            attribute2: CAST(Yson::ConvertToString($data.attribute2) AS Utf8),
            change_time : Unwrap(cast(datetime::MakeDatetime(datetime::ParseIso8601(Yson::ConvertToString($data.change_time))) as timestamp)),
            operation: cast($operation_type as Utf8)
        |>, NULL);

    return $result;
  };

  CREATE TRANSFER dimension_scd2_cdc_changes
        FROM `source_customers2/updates` TO dimension_scd_changes USING $transformation_lambda
        with(FLUSH_INTERVAL=Interval("PT1S"))
        ;

  ```

  4. Создайте периодический запрос для подмерживания изменений из таблицы `dimension_scd_changes` в основную таблицу хранения данных в формате SCD2.

  Детально процесс построения SCD2 структур данных описан в разделе [SCD2](../scd2.md).


{% endlist %}
