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
  | `Int16` | `int16` | |
  | `Int32` | `int32` | |
  | `Int64` | `int64` | |
  | `Uint8` | `uint8` | |
  | `Uint16` | `uint16` | |
  | `Uint32` | `uint32` | |
  | `Uint64` | `uint64` | |
  | `Float` | `float` | |
  | `Double` | `double` | |
  | `Decimal(p, s)` | `fixed_size_binary(16)` | Используются 120 бит, есть маркеры ±inf и NaN |
  | `DyNumber` | `string` | Строковое представление числа |

  *Строковые типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `String` | `binary` | |
  | `Utf8` | `string` | |
  | `Json` | `string` | |
  | `JsonDocument` | `string` | Строкое представление бинарного [JSON](https://en.wikipedia.org/wiki/JSON) |
  | `Yson` | `binary` | |
  | `Uuid` | `fixed_size_binary(16)` | 16 байт в mixed-endian порядке |

  *Временные типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Date` | `uint16` | |
  | `Date32` | `int32` | |
  | `Datetime` | `uint32` | |
  | `Datetime64` | `int64` | |
  | `Timestamp` | `uint64` | |
  | `Timestamp64` | `int64` | |
  | `Interval` | `int64` | |
  | `Interval64` | `int64` | |
  | `TzDate` | `struct<datetime: uint16, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzDate32` | `struct<datetime: int32, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzDatetime` | `struct<datetime: uint32, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzDatetime64` | `struct<datetime: int64, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzTimestamp` | `struct<datetime: uint64, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzTimestamp64` | `struct<datetime: int64, timezone: string>` | Включает строковое имя метки таймзоны |

  *Сингулярные типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Null` | `null` | |
  | `Void` | `struct<>` | |
  | `EmptyList` | `struct<>` | |
  | `EmptyDict` | `struct<>` | |

  *Составные типы*

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Optional<T>` | `struct<opt: T>` | Если T является `Variant`, `Optional`, `Pg` или сингулярным типом |
  | `Optional<T>` | `T` | Для всех остальных типов |
  | `List<T>` | `list<T>` | |
  | `Tuple<T1, T2, ...>` | `struct<field0: T1, field1: T2, ...>` | |
  | `Struct<name: T, ...>` | `struct<name: T, ...>` | |
  | `Dict<K, V>` | `list<struct<key: K, payload: V>>` | |
  | `Variant<T1, ..., Tn>` | `dense_union<field0: T1, ...>` | Для n <= 128 |
  | `Variant<T1, ..., Tn>` | `dense_union<dense_union<field0: T1, ...>, ...>` | Для 128 < n <= 16384, не поддерживается для n > 16384 |
  | `Tagged<T>` | `T` | |

  *Типы семейства pg*

  Все типы семейства `pg` возвращаются как Arrow тип `string` в формате текстового представления значений.

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
