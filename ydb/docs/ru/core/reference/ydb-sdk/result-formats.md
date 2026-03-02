# Формат данных в результате запроса

{{ ydb-short-name }} поддерживает несколько форматов представления данных и режимов возвращения схемы в результате выполнения запроса через QueryService. Формат и режим возвращения схемы задаются при выполнении запроса и распространяются на каждую инструкцию, которая возвращает данные (например, инструкция SELECT или ключевое слово RETURNING). Конфигурация данных настроек напрямую зависит от SDK.

## Форматы результатов {#result-format}

{% list tabs %}

- Value

  Формат по умолчанию. Данные возвращаются в виде типизированных значений {{ ydb-short-name }} и преобразовываются на стороне SDK.

  Этот формат подходит для большинства задач, когда данные обрабатываются построчно в клиентском приложении.

- Apache Arrow

  Данные возвращаются в виде списка сериализованных [Apache Arrow](https://arrow.apache.org/) RecordBatch (формат IPC версии 5.0) и не преобразовываются на стороне SDK. Apache Arrow — это колоночный формат данных в памяти, оптимизированный для аналитических операций и обмена данными между системами.

  Этот формат рекомендуется для:

  * Аналитических задач, где данные обрабатываются колоночно;
  * Систем, которые нативно работают с Apache Arrow;
  * Задач, где важна высокая производительность при передаче больших объёмов данных.

  **Сжатие данных**

  Для формата Apache Arrow можно настроить сжатие передаваемых данных. Доступны следующие кодеки:

  | Кодек | Описание |
  |-------|----------|
  | Без сжатия (по умолчанию) | Данные передаются без сжатия |
  | `ZSTD` | Сжатие [Zstandard](https://github.com/facebook/zstd). Поддерживает настройку уровня сжатия |
  | `LZ4_FRAME` | Сжатие [LZ4](https://github.com/lz4/lz4). Настройка уровня сжатия не поддерживается |

  **Конвертация типов данных YQL в Apache Arrow**

  Типы данных YQL конвертируются в типы Apache Arrow по следующим правилам.

  *Числовые типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Bool` | `uint8` | Значения `true`/`false` кодируются как `1`/`0` |
  | `Int8` | `int8` | |
  | `Uint8` | `uint8` | |
  | `Int16` | `int16` | |
  | `Uint16` | `uint16` | |
  | `Int32` | `int32` | |
  | `Uint32` | `uint32` | |
  | `Int64` | `int64` | |
  | `Uint64` | `uint64` | |
  | `Float` | `float32` | |
  | `Double` | `float64` | |
  | `Decimal(p, s)` | `decimal128(p, s)` | Точность и масштаб сохраняются |

  *Строковые типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Utf8` | `utf8` | Строки в кодировке UTF-8 |
  | `Json` | `utf8` | Строковое представление JSON |

  *Бинарные типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `String` | `binary` | Произвольные байтовые данные |
  | `Yson` | `binary` | Двоичное представление YSON |
  | `JsonDocument` | `utf8` | Декодированное строковое представление JSON |
  | `DyNumber` | `binary` | Строковое представление числа |
  | `Uuid` | `binary` | 16 байт в little-endian порядке |

  *Временные типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Date` | `uint16` | Количество дней с 1 января 1970 года |
  | `Datetime` | `uint32` | Количество секунд с 1 января 1970 года |
  | `Timestamp` | `uint64` | Количество микросекунд с 1 января 1970 года |
  | `Interval` | `int64` | Длительность в микросекундах |

  *Составные типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Optional<T>` | `struct<opt: T>` | Обёртка с одним nullable-полем |
  | `Optional<Optional<T>>` | `struct<opt: struct<opt: T>>` | Каждый уровень Optional добавляет уровень вложенности |
  | `List<T>` | `list<T>` | Список элементов типа T |
  | `Tuple<T1, T2, ...>` | `struct<T1, T2, ...>` | Структура с позиционными полями |
  | `Struct<name: T, ...>` | `struct<name: T, ...>` | Структура с именованными полями |
  | `Dict<K, V>` | `list<struct<key: K, value: V>>` | Список пар ключ-значение |
  | `Variant<name: T, ...>` | `dense_union<name: T, ...>` | Дискриминируемое объединение |

{% endlist %}

## Режимы возвращения схемы {#schema-inclusion-mode}

При потоковом выполнении запроса результаты передаются частями. Режим возвращения схемы определяет, в каких частях потока содержится информация о схеме данных.

* **Always (по умолчанию)** — схема данных возвращается в каждой части потока ответов. Это удобно для простой обработки результатов, когда каждая часть обрабатывается независимо.
* **First only** — схема данных возвращается только в первой части потока для каждого результирующего набора. В последующих частях схема отсутствует. Этот режим позволяет уменьшить объём передаваемых данных при потоковой обработке больших результатов.

Поведение режима возвращения схемы в том числе зависит от указанного формата данных в результате запроса.

{% list tabs %}

- Value

  Результат содержит только схему со списком столбцов с YQL-типами.

- Apache Arrow

  Результат содержит 2 схемы: список столбцов с YQL-типами и сериализованную схему Arrow RecordBatch.

{% endlist %}

## Примеры использования в SDK {#sdk-examples}

{% list tabs group=lang %}

- Python

  ```python
  pool = ydb.QuerySessionPool(driver)

  query = """
      SELECT * FROM example ORDER BY Key LIMIT 100;
  """

  format_settings = ydb.ArrowFormatSettings(
      compression_codec=ydb.ArrowCompressionCodec(ydb.ArrowCompressionCodecType.ZSTD, 10)
  )

  result = pool.execute_with_retries(
      query,
      result_set_format=ydb.QueryResultSetFormat.ARROW,
      schema_inclusion_mode=ydb.QuerySchemaInclusionMode.FIRST_ONLY,
      arrow_format_settings=format_settings,
  )

  for result_set in result:
      schema = pyarrow.ipc.read_schema(pyarrow.py_buffer(result_set.arrow_format_meta.schema))
      batch = pyarrow.ipc.read_record_batch(pyarrow.py_buffer(result_set.data), schema)
      print(f"Record batch with {batch.num_rows} rows and {batch.num_columns} columns")
  ```

- Java

  ```java
  BatchAssert ba = new BatchAssert(COLUMN_TABLE, COLUMN_BATCH);

  String query = selectTableYql(COLUMN_TABLE_NAME);
  ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
          .useApacheArrowFormat(ApacheArrowFormat.zstd())
          .build();

  try (QuerySession session = client.createSession(Duration.ofSeconds(5)).join().getValue()) {
      QueryStream stream = session.createQuery(query, TxMode.SNAPSHOT_RO, Params.empty(), settings);
      assertStatusOK(stream.execute(new ApacheArrowCompressedPartsHandler(allocator) {
          @Override
          public void onNextPart(QueryResultPart part) {
              Assert.assertTrue(part instanceof ApacheArrowQueryResultPart);
              Assert.assertEquals(0, part.getResultSetIndex());
              ba.assertResultSetReader(part.getResultSetReader());
          }
      }).join().getStatus());
      ba.assertFinish();
  }
  ```

{% endlist %}
