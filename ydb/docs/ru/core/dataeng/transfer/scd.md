# Реализация SCD с использованием трансфера данных в YDB

В этом руководстве рассматривается реализация паттернов Slowly Changing Dimensions ([SCD](https://ru.wikipedia.org/wiki/Медленно_меняющееся_измерение)) типа 1 и 2 с использованием механизма трансфера данных YDB.

## Общие сведения о трансфере данных

Трансфер в YDB — асинхронный механизм переноса данных из топика в таблицу. Он осуществляет чтение сообщений из указанного топика, их преобразование с помощью lambda-функции и запись в целевую таблицу. Если в таблице уже есть строка с указанным первичным ключом, то строка будет обновлена.

Обратите внимание, что трансфер **не может удалять данные** из таблицы. Он может только добавлять новые строки или обновлять существующие.

## Реализация SCD1 с использованием трансфера

### Особенности SCD1
SCD1 (Type 1) — это подход, при котором при изменении атрибута измерения старое значение заменяется новым. Хранится только текущее состояние данных. Этот подход используется, когда:
- Историческая информация не требуется
- Важно иметь только актуальные данные
- Необходимо минимизировать размер хранилища данных
- Требуется простая структура данных для аналитики

### Реализация SCD1 через трансфер

{% list tabs %}

- Стандартные бизнес-данные

  1. Создайте таблицу с первичным ключом, соответствующим бизнес-ключу ваших данных:

    ```sql
    CREATE TABLE dimension_scd1 (
        business_key String NOT NULL,
        attribute1 String,
        attribute2 String,
        source_system String,
        is_deleted Bool,
        last_update_timestamp Timestamp,
        etl_batch_id String,
        PRIMARY KEY (business_key)
    );
    ```

  2. Создайте lambda-функцию, которая будет преобразовывать сообщения из топика в строки таблицы, и трансфер:

    ```sql
    $transformation_lambda = ($msg) -> {
      $data = CAST($msg._data AS JSON);

      return [
          <|
              business_key: Unwrap(CAST(Yson::ConvertToString($data.business_key) AS String)),
              attribute1: CAST(Yson::ConvertToString($data.attribute1) AS String),
              attribute2: CAST(Yson::ConvertToString($data.attribute2) AS String),
              source_system: CAST(Yson::ConvertToString($data.source_system) AS String),
              last_update_timestamp: $msg._write_timestamp,
              etl_batch_id: CAST(Yson::ConvertToString($data.etl_batch_id) AS String)
          |>
      ];
    };

    CREATE TRANSFER dimension_scd1_transfer
      FROM dimension_topic TO dimension_scd1 USING $transformation_lambda;
    ```

    При поступлении новых данных для существующего бизнес-ключа, трансфер автоматически создаст или обновит соответствующую строку в целевой таблице.

    {% note info %}

    Обратите внимание, что данные из трансфера в таблицу попадают с задержкой, определяемой временем буферизации данных (!!СОСЛАТЬСЯ НА РЕАЛЬНУЮ ДОКУМЕНТАЦИЮ!!!) в настройках трансфера.

    {% endnote %}

  3. Отправьте в топик данные

    Ниже приведен пример JSON-данных, которые нужно записать в топик для обработки lambda-функцией:

    Запишите в топик
    ```bash
    ydb -e <YDB_ENDPOINT> -d <YDB_DATABASE> topic write dimension_topic
    ```
    следующие данные
    ```json
    {
      "business_key": "CUSTOMER_1001",
      "attribute1": "John Doe",
      "attribute2": "New York",
      "source_system": "CRM",
      "etl_batch_id": "BATCH_20250101_001"
    }
    ```

    При записи этих данных в топик, lambda-функция преобразует их в соответствующие записи таблицы. Если в таблице данные уже были, они будут обновлены на вновь пришедшие.

- CDC данные YDB (формат Debezium)

  Для работы с CDC данными YDB в формате Debezium требуется специальная обработка, так как формат содержит метаданные об изменениях и структуру с полями `before` и `after`:

  1. Создайте таблицу:

  ```sql
  CREATE TABLE dimension_scd1 (
      business_key String NOT NULL,
      attribute1 String,
      attribute2 String,
      source_system String,
      is_deleted Bool,
      last_update_timestamp Timestamp,
      operation_type String,
      PRIMARY KEY (business_key)
  );
  ```

  2. Создайте lambda-функцию для обработки CDC данных и трансфер

    ```sql
    $transformation_lambda = ($msg) -> {
        $cdc_data = CAST($msg._data AS JSON);

        -- Определяем тип операции
        $operation = Yson::ConvertToString($cdc_data.payload.op);
        $is_deleted = $operation == "d"; -- d - delete, u - update

        -- Получаем данные в зависимости от типа операции
        $data = IF($is_deleted, $cdc_data.payload.before, $cdc_data.payload.after);

        return [
            <|
                business_key: Unwrap(CAST(Yson::ConvertToString($data.id) AS String)),
                attribute1: CAST(Yson::ConvertToString($data.attribute1) AS String),
                attribute2: CAST(Yson::ConvertToString($data.attribute2) AS String),
                source_system: "YDB_CDC",
                is_deleted: $is_deleted,
                last_update_timestamp: CAST(Yson::ConvertToString($cdc_data.payload.ts_ms) AS Timestamp),
                operation_type: $operation
            |>
        ];
    };

        CREATE TRANSFER dimension_scd1_cdc_transfer
          FROM `source_customers/updates` TO dimension_scd1_cdc USING $transformation_lambda;
    ```

  ### Пример таблицы-источника и работы с CDC

  Для демонстрации работы с CDC данными, создадим таблицу-источник в YDB, которая будет генерировать CDC-события:

  ```sql
  -- Создание таблицы-источника
  CREATE TABLE source_customers (
      id String NOT NULL,
      attribute1 String,
      attribute2 String,
      updated_at Timestamp,
      PRIMARY KEY (id)
  );
  ```

  Теперь выполним операции с данными, которые будут генерировать CDC-события:

  **1. Вставка новой записи (создаст событие с `op = "u"`):**

  ```sql
  -- Вставка новой записи
  INSERT INTO source_customers (id, attribute1, attribute2, updated_at)
  VALUES ('CUSTOMER_1001', 'John Doe', 'New York', CurrentUtcTimestamp());
  ```

  Это действие создаст CDC-событие примерно следующего вида:

  ```json
  {
    "payload": {
      "after": {
        "id": "CUSTOMER_1001",
        "attribute1": "John Doe",
        "attribute2": "New York",
        "updated_at": "2025-08-18T18:21:38.706921Z"
      },
      "source": { ... },
      "op": "u",
      "ts_ms": 1735689600000
    }
  }
  ```

  **2. Обновление существующей записи (создаст событие с `op = "u"`):**

  ```sql
  -- Обновление существующей записи
  UPSERT INTO source_customers (id, attribute1, attribute2, updated_at)
  VALUES ('CUSTOMER_1001', 'John Doe', 'Los Angeles', CurrentUtcTimestamp());
  ```

  Это действие создаст CDC-событие примерно следующего вида:

  ```json
  {
    "schema": { ... },
    "payload": {
      "before": {
        "id": "CUSTOMER_1001",
        "attribute1": "John Doe",
        "attribute2": "New York",
        "updated_at": "2025-01-01T12:00:00Z"
      },
      "after": {
        "id": "CUSTOMER_1001",
        "attribute1": "John Doe",
        "attribute2": "Los Angeles",
        "updated_at": "2025-01-02T12:00:00Z"
      },
      "source": { ... },
      "op": "u",
      "ts_ms": 1735776000000
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
    "schema": { ... },
    "payload": {
      "before": {
        "id": "CUSTOMER_1001",
        "attribute1": "John Doe",
        "attribute2": "Los Angeles",
        "updated_at": "2025-01-02T12:00:00Z"
      },
      "after": null,
      "source": { ... },
      "op": "d",
      "ts_ms": 1735862400000
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
  * При удалении (`op = "d"`) данные находятся в поле `before`, а при создании/обновлении - в поле `after`
  * Необходимо корректно обрабатывать NULL-значения, которые могут возникать в различных сценариях

{% endlist %}

### Дополнительные рекомендации для SCD1

1. **Добавьте метаданные**: Включите в таблицу дополнительные поля для отслеживания источника данных, времени загрузки, идентификатора пакета ETL и т.д.

1. **Используйте составные ключи при необходимости**: Если бизнес-ключ состоит из нескольких полей, используйте составной первичный ключ.

1. **Добавьте валидацию данных**: В lambda-функции можно реализовать базовую валидацию данных и пропускать некорректные записи.

1. **Логируйте отброшенные записи**: Если запись не прошла валидацию, полезно сохранить её в отдельную таблицу для последующего анализа.

## Реализация SCD2 (append-only) с использованием трансфера

### Особенности SCD2 и подход append-only

SCD2 (Type 2) — это подход, при котором при изменении атрибута измерения создается новая запись, а старая помечается как неактуальная. Таким образом, сохраняется история изменений. Этот подход используется, когда:
- Требуется отслеживать историю изменений данных
- Необходимо выполнять анализ данных с учетом временных периодов
- Важно сохранять аудиторский след изменений
- Требуется возможность восстановления состояния данных на определенный момент времени

В YDB трансфере для реализации SCD2 используется подход **append-only** (только добавление). Это означает, что при каждом изменении данных создается новая запись, а старые записи никогда не удаляются и не изменяются физически. Вместо этого они помечаются как неактуальные с помощью специальных флагов и временных меток.

Подход append-only имеет несколько важных преимуществ:

1. **Производительность**: Операции удаления в колоночных таблицах, которые используются в YDB, являются дорогостоящими с точки зрения производительности. Добавление новых записей вместо удаления существующих значительно эффективнее.

2. **Согласованность**: Поскольку старые данные никогда не удаляются, всегда можно восстановить состояние данных на любой момент времени.

3. **Масштабируемость**: Добавление новых записей можно легко распараллелить, что обеспечивает хорошую масштабируемость при больших объемах данных.

Однако у этого подхода есть и недостатки, такие как увеличение объема хранимых данных и необходимость дополнительной логики для определения актуальных записей. Для эффективной работы с SCD2 в режиме append-only требуется правильное проектирование схемы данных и дополнительные процессы для обслуживания таблиц.

### Реализация SCD2 через трансфер (append-only)

{% list tabs %}

- Стандартные бизнес-данные

  Для реализации SCD2 в режиме append-only (только добавление новых записей) с использованием трансфера:

  1. Создайте таблицу с составным первичным ключом, включающим бизнес-ключ и версию или временную метку:

  ```sql
  CREATE TABLE dimension_scd2 (
      business_key String NOT NULL,
      valid_from Timestamp NOT NULL,
      valid_to Timestamp,
      attribute1 String,
      attribute2 String,
      source_system String,
      is_current Bool,
      is_deleted Bool,
      etl_batch_id String,
      PRIMARY KEY (business_key, valid_from)
  );
  ```

  2. Создайте lambda-функцию, которая будет добавлять новые версии записей:

  ```sql
  $transformation_lambda = ($msg) -> {
      $data = CAST($msg._data AS JSON);
      $current_timestamp = $msg._write_timestamp;
      $business_key = CAST(Yson::ConvertToString($data.business_key) AS String);
      $is_deleted = false; // Для бизнес-данных не используем операцию удаления
      $operation_type = CAST(Yson::ConvertToString($data.operation_type) AS String);

      // Результирующий массив записей
      $result = [];

      // Если это обновление и у нас есть старые данные (режим NEW_AND_OLD_IMAGES)
      if ($operation_type == "UPDATE" && $data.old_data != NULL) {
          // Старая запись (закрываем период актуальности)
          $old_data = $data.old_data;
          $result += <|
              business_key: $business_key,
              valid_from: CAST(Yson::ConvertToString($old_data.valid_from) AS Timestamp),
              valid_to: $current_timestamp,   // valid_to = valid_from новой записи
              attribute1: CAST(Yson::ConvertToString($old_data.attribute1) AS String),
              attribute2: CAST(Yson::ConvertToString($old_data.attribute2) AS String),
              source_system: CAST(Yson::ConvertToString($old_data.source_system) AS String),
              is_current: false,      // Помечаем как неактуальную
              is_deleted: $is_deleted,
              etl_batch_id: CAST(Yson::ConvertToString($data.etl_batch_id) AS String)
          |>;
      }

      // Создаем новую актуальную запись
      $new_record = <|
          business_key: $business_key,
          valid_from: $current_timestamp,
          valid_to: NULL,
          attribute1: CAST(Yson::ConvertToString($data.attribute1) AS String),
          attribute2: CAST(Yson::ConvertToString($data.attribute2) AS String),
          source_system: CAST(Yson::ConvertToString($data.source_system) AS String),
          is_current: true,
          is_deleted: $is_deleted,
          etl_batch_id: CAST(Yson::ConvertToString($data.etl_batch_id) AS String)
      |>;

      $result += $new_record;

      return $result;
  };
  ```

  3. Создайте трансфер:

  ```sql
  CREATE TRANSFER dimension_scd2_transfer
      FROM dimension_topic TO dimension_scd2 USING $transformation_lambda;
  ```

  {% note info %}

  Для корректной работы с обновлениями в SCD2 необходимо, чтобы в сообщениях приходили как новые данные, так и старые значения (в поле `old_data`). Это позволяет lambda-функции создавать две записи: одну для закрытия старой версии и одну для новой версии, без необходимости дополнительной логики вне трансфера.

  {% endnote %}

  4. Обновление флагов `is_current` и значений `valid_to` в подходе append-only

  В подходе append-only мы стремимся избегать обновления существующих записей. Однако, нам все равно нужно как-то определять, какие записи являются актуальными. Для этого существует несколько подходов:

  #### Подход 1: Использование представления (view)

  Вместо физического обновления флагов `is_current` и значений `valid_to`, можно создать представление, которое будет динамически определять эти значения:

  ```sql
  CREATE VIEW dimension_scd2_current AS
  SELECT
      d.*,
      NOT EXISTS (
          SELECT 1
          FROM dimension_scd2 AS newer
          WHERE
              newer.business_key = d.business_key AND
              newer.valid_from > d.valid_from
      ) AS is_current,
      (
          SELECT MIN(valid_from)
          FROM dimension_scd2 AS newer
          WHERE
              newer.business_key = d.business_key AND
              newer.valid_from > d.valid_from
      ) AS valid_to
  FROM dimension_scd2 AS d;
  ```

  Преимущества этого подхода:
  * Полностью соответствует принципу append-only (никаких обновлений)
  * Всегда актуальные данные
  * Простота реализации

  Недостатки:
  * Может быть менее производительным при больших объемах данных
  * Требует выполнения подзапросов при каждом обращении

  #### Подход 2: Периодическое обновление через создание новой таблицы

  Если вы хотите избежать обновления существующих записей, но при этом иметь более производительные запросы, можно периодически создавать новую таблицу с актуальными значениями:

  ```sql
  -- Создаем новую таблицу с актуальными значениями
  CREATE TABLE dimension_scd2_new AS
  SELECT
      d.*,
      NOT EXISTS (
          SELECT 1
          FROM dimension_scd2 AS newer
          WHERE
              newer.business_key = d.business_key AND
              newer.valid_from > d.valid_from
      ) AS is_current,
      (
          SELECT MIN(valid_from)
          FROM dimension_scd2 AS newer
          WHERE
              newer.business_key = d.business_key AND
              newer.valid_from > d.valid_from
      ) AS valid_to
  FROM dimension_scd2 AS d;

  -- Переименовываем таблицы
  RENAME TABLE dimension_scd2 TO dimension_scd2_old, dimension_scd2_new TO dimension_scd2;

  -- Удаляем старую таблицу
  DROP TABLE dimension_scd2_old;
  ```

  Преимущества этого подхода:
  * Соответствует принципу append-only (нет обновлений существующих записей)
  * Более производительные запросы к актуальным данным
  * Возможность оптимизации структуры таблицы при пересоздании

  Недостатки:
  * Требует дополнительного места для хранения
  * Временное удвоение объема данных при пересоздании
  * Необходимость обновления ссылок на таблицу в других объектах

  #### Подход 3: Традиционное обновление (не строго append-only)

  Если производительность критична, а объем данных не позволяет использовать предыдущие подходы, можно использовать традиционное обновление:

  ```sql
  UPDATE dimension_scd2
  SET
      valid_to = (
          SELECT MIN(valid_from)
          FROM dimension_scd2 AS newer
          WHERE
              dimension_scd2.business_key = newer.business_key AND
              newer.valid_from > dimension_scd2.valid_from
      ),
      is_current = false
  WHERE
      is_current = true AND
      EXISTS (
          SELECT 1
          FROM dimension_scd2 AS newer
          WHERE
              dimension_scd2.business_key = newer.business_key AND
              newer.valid_from > dimension_scd2.valid_from
      );
  ```

  Этот подход не является строго append-only, но может быть приемлемым компромиссом в некоторых сценариях.

  Для запросов к текущим данным используйте фильтр `WHERE is_current = true AND is_deleted = false`.

  ### Примеры данных для записи в топик

  Ниже приведены примеры JSON-данных, которые нужно записать в топик для обработки lambda-функцией в SCD2:

  **Создание новой записи:**
  ```json
  {
    "business_key": "PRODUCT_2001",
    "attribute1": "Laptop XPS",
    "attribute2": "1299.99",
    "source_system": "ERP",
    "operation_type": "INSERT",
    "etl_batch_id": "BATCH_20250101_001"
  }
  ```

  **Обновление существующей записи (создаст новую версию):**
  ```json
  {
    "business_key": "PRODUCT_2001",
    "attribute1": "Laptop XPS",
    "attribute2": "1199.99",
    "source_system": "ERP",
    "operation_type": "UPDATE",
    "etl_batch_id": "BATCH_20250102_001"
  }
  ```

  При записи этих данных в топик, lambda-функция создаст новую запись для каждого сообщения. Для операций создания и обновления трансфер автоматически добавит новую запись.

  Обратите внимание, что для закрытия периодов актуальности предыдущих версий требуется дополнительная периодическая задача, как описано выше.

- CDC данные YDB (формат Debezium)

  Для работы с CDC данными YDB в формате Debezium при реализации SCD2:

  1. Создайте таблицу с той же структурой:

  ```sql
  CREATE TABLE dimension_scd2 (
      business_key String NOT NULL,
      valid_from Timestamp NOT NULL,
      valid_to Timestamp,
      attribute1 String,
      attribute2 String,
      source_system String,
      is_current Bool,
      is_deleted Bool,
      operation_type String,
      PRIMARY KEY (business_key, valid_from)
  );
  ```

  2. Создайте lambda-функцию для обработки CDC данных в формате Debezium:

  ```sql
  $transformation_lambda = ($msg) -> {
      $cdc_data = CAST($msg._data AS JSON);

      // Определяем тип операции
      $operation = Yson::ConvertToString($cdc_data.payload.op);
      $is_deleted = $operation == "d"; // d - delete, c - create, u - update

      // Используем timestamp из сообщения или текущее время
      $timestamp = $cdc_data.payload.ts_ms != NULL ?
          CAST(Yson::ConvertToString($cdc_data.payload.ts_ms) AS Timestamp) :
          CurrentUtcTimestamp();

      // Результирующий массив записей
      $result = [];

      // Для операции обновления (u) нам нужно создать две записи:
      // 1. Закрыть старую версию (is_current = false)
      // 2. Создать новую версию (is_current = true)
      if ($operation == "u" && $cdc_data.payload.before != NULL && $cdc_data.payload.after != NULL) {
          // Старая запись (закрываем период актуальности)
          $old_data = $cdc_data.payload.before;
          $result += <|
              business_key: CAST(Yson::ConvertToString($old_data.id) AS String),
              valid_from: $timestamp, // Используем тот же timestamp для обеих записей
              valid_to: $timestamp,   // valid_to = valid_from новой записи
              attribute1: CAST(Yson::ConvertToString($old_data.attribute1) AS String),
              attribute2: CAST(Yson::ConvertToString($old_data.attribute2) AS String),
              source_system: "YDB_CDC",
              is_current: false,      // Помечаем как неактуальную
              is_deleted: false,
              operation_type: $operation
          |>;

          // Новая запись (актуальная версия)
          $new_data = $cdc_data.payload.after;
          $result += <|
              business_key: CAST(Yson::ConvertToString($new_data.id) AS String),
              valid_from: $timestamp,
              valid_to: NULL,
              attribute1: CAST(Yson::ConvertToString($new_data.attribute1) AS String),
              attribute2: CAST(Yson::ConvertToString($new_data.attribute2) AS String),
              source_system: "YDB_CDC",
              is_current: true,       // Помечаем как актуальную
              is_deleted: false,
              operation_type: $operation
          |>;
      }
      // Для операции удаления создаем запись с is_deleted = true
      else if ($is_deleted && $cdc_data.payload.before != NULL) {
          $data = $cdc_data.payload.before;
          $result += <|
              business_key: CAST(Yson::ConvertToString($data.id) AS String),
              valid_from: $timestamp,
              valid_to: NULL,
              attribute1: CAST(Yson::ConvertToString($data.attribute1) AS String),
              attribute2: CAST(Yson::ConvertToString($data.attribute2) AS String),
              source_system: "YDB_CDC",
              is_current: true,
              is_deleted: true,
              operation_type: $operation
          |>;
      }
      // Для операции создания или если нет данных before/after
      else if (!$is_deleted && $cdc_data.payload.after != NULL) {
          $data = $cdc_data.payload.after;
          $result += <|
              business_key: CAST(Yson::ConvertToString($data.id) AS String),
              valid_from: $timestamp,
              valid_to: NULL,
              attribute1: CAST(Yson::ConvertToString($data.attribute1) AS String),
              attribute2: CAST(Yson::ConvertToString($data.attribute2) AS String),
              source_system: "YDB_CDC",
              is_current: true,
              is_deleted: false,
              operation_type: $operation
          |>;
      }

      return $result;
  };
  ```

  3. Создайте трансфер:

  ```sql
  CREATE TRANSFER dimension_scd2_cdc_transfer
      FROM ydb_cdc_topic TO dimension_scd2 USING $transformation_lambda;
  ```

  4. Создайте периодическую задачу для обновления флагов `is_current` и значений `valid_to` (аналогично предыдущему примеру).

  {% note warning %}

  Для корректной работы с CDC данными в SCD2 необходимо включить режим, при котором в CDC сообщениях приходят полные значения ключей как для старых (`before`), так и для новых (`after`) данных. Это критично для правильного связывания записей и обеспечения целостности истории изменений.

  {% endnote %}

  Особенности обработки CDC данных для SCD2:

  * Важно сохранять точную временную метку из сообщения CDC для корректного отслеживания истории
  * При работе с CDC необходимо учитывать возможность получения сообщений не в хронологическом порядке
  * Для операций удаления создается новая запись с флагом `is_deleted = true`, что позволяет отслеживать историю удалений
  * Периодическая задача обновления флагов должна учитывать специфику CDC данных, особенно при обработке out-of-order событий
  * Lambda-функция обрабатывает как старые, так и новые данные, создавая две записи при обновлении: одну для закрытия старой версии и одну для новой версии

{% endlist %}

### Альтернативные подходы к SCD2

{% list tabs %}

- Подход с хэшем содержимого

  Использование хэша содержимого для определения изменений позволяет избежать создания дубликатов:

  1. Создайте таблицу с хэшем содержимого в первичном ключе:

  ```sql
  CREATE TABLE dimension_scd2_hash (
      business_key String NOT NULL,
      content_hash String NOT NULL,
      valid_from Timestamp NOT NULL,
      valid_to Timestamp,
      attribute1 String,
      attribute2 String,
      source_system String,
      is_current Bool,
      is_deleted Bool,
      PRIMARY KEY (business_key, content_hash)
  );
  ```

  2. Создайте lambda-функцию, которая будет вычислять хэш содержимого:

  ```sql
  $transformation_lambda = ($msg) -> {
      $data = CAST($msg._data AS JSON);
      $business_key = CAST(Yson::ConvertToString($data.business_key) AS String);
      $attribute1 = CAST(Yson::ConvertToString($data.attribute1) AS String);
      $attribute2 = CAST(Yson::ConvertToString($data.attribute2) AS String);
      $is_deleted = CAST(Yson::ConvertToString($data.operation_type) AS String) == "DELETE";

      // Вычисляем хэш содержимого (упрощенный пример)
      $content = $attribute1 + "|" + $attribute2;
      $content_hash = Digest::MD5($content);

      // Проверяем, нужно ли создавать новую запись
      // Если запись с таким хэшем уже существует, новая не будет создана
      // из-за ограничения первичного ключа

      return [
          <|
              business_key: $business_key,
              content_hash: $content_hash,
              valid_from: $msg._write_timestamp,
              valid_to: NULL,
              attribute1: $attribute1,
              attribute2: $attribute2,
              source_system: CAST(Yson::ConvertToString($data.source_system) AS String),
              is_current: true,
              is_deleted: $is_deleted
          |>
      ];
  };
  ```

  Преимущества этого подхода:
  * Автоматическое предотвращение дубликатов
  * Эффективное хранение данных (только уникальные состояния)
  * Упрощение логики обработки повторных сообщений

  Недостатки:
  * Сложность отслеживания точной хронологии изменений
  * Необходимость дополнительного процесса для обновления флагов `is_current` и значений `valid_to`

- Подход с версионированием

  Альтернативный подход с использованием версий вместо временных меток:

  1. Создайте таблицу с версией в первичном ключе:

  ```sql
  CREATE TABLE dimension_scd2_version (
      business_key String NOT NULL,
      version Uint64 NOT NULL,
      valid_from Timestamp NOT NULL,
      valid_to Timestamp,
      attribute1 String,
      attribute2 String,
      is_current Bool,
      is_deleted Bool,
      PRIMARY KEY (business_key, version)
  );
  ```

  2. Создайте дополнительную таблицу для отслеживания текущих версий:

  ```sql
  CREATE TABLE dimension_version_tracker (
      business_key String NOT NULL,
      current_version Uint64,
      PRIMARY KEY (business_key)
  );
  ```

  3. Реализуйте логику инкремента версий через дополнительный процесс перед трансфером.

  Этот подход упрощает запросы к историческим данным и обеспечивает более строгий контроль версионирования, но требует дополнительной инфраструктуры для управления версиями.

{% endlist %}

## Рекомендации по работе с SCD через трансфер

### Общие рекомендации

1. **Используйте временные метки из сообщений**: Для точного отслеживания времени изменений используйте `_write_timestamp` или `_create_timestamp` из сообщения, а для CDC данных - метку времени из самого сообщения CDC.

2. **Группируйте сообщения по бизнес-ключу**: Рекомендуется группировать сообщения, относящиеся к одной строке таблицы, в одну партицию топика для сохранения порядка обработки.

3. **Обрабатывайте ошибки**: Настройте мониторинг трансфера для своевременного обнаружения ошибок преобразования данных. Особенно важно отслеживать ошибки при работе с CDC данными, где формат может быть сложнее.

4. **Оптимизируйте параметры батчевания**: Для больших объемов данных настройте параметры `BATCH_SIZE_BYTES` и `FLUSH_INTERVAL` для оптимального баланса между задержкой и производительностью.

5. **Используйте важных читателей**: Если вы временно останавливаете трансфер, сделайте читателя важным, чтобы гарантировать, что необработанные сообщения не будут удалены из топика.

### Рекомендации для работы с CDC данными

1. **Обрабатывайте события вне порядка**: CDC системы могут доставлять события не в хронологическом порядке. Разработайте стратегию обработки таких ситуаций, особенно для SCD2.

2. **Учитывайте специфику формата Debezium**: Формат Debezium имеет определенную структуру с полями `before`, `after`, `op` и другими метаданными. Убедитесь, что ваша lambda-функция корректно обрабатывает все возможные варианты.

3. **Обрабатывайте NULL-значения**: В CDC данных могут встречаться NULL-значения в различных полях. Определите политику их обработки.

4. **Реализуйте идемпотентность**: CDC системы могут доставлять одно и то же сообщение несколько раз. Используйте подход с хэшем содержимого или другие механизмы для обеспечения идемпотентности.

5. **Отслеживайте схему данных**: Схема данных в источнике может меняться со временем. Разработайте стратегию обработки изменений схемы.

## Заключение

Трансфер данных в YDB предоставляет мощный механизм для реализации паттернов SCD1 и SCD2, несмотря на ограничение невозможности удаления данных. Для SCD1 можно использовать прямое обновление строк с добавлением флага `is_deleted` для отслеживания логических удалений. Для SCD2 в режиме append-only требуется дополнительный процесс для обновления периодов актуальности, но базовая функциональность добавления новых версий может быть реализована через трансфер.

Работа с CDC данными в формате Debezium добавляет дополнительный уровень сложности, но предоставляет возможность создания надежных систем отслеживания изменений данных. Правильная обработка CDC данных позволяет реализовать как SCD1, так и SCD2 паттерны с минимальной задержкой и высокой точностью.

Альтернативные подходы, такие как использование хэша содержимого или версионирование, могут быть полезны в специфических сценариях и предоставляют дополнительные возможности для оптимизации хранения и обработки данных.

При правильном проектировании схемы данных и lambda-функций преобразования, трансфер становится эффективным инструментом для построения хранилищ данных с поддержкой медленно меняющихся измерений, обеспечивая как актуальность данных (SCD1), так и сохранение истории изменений (SCD2).
