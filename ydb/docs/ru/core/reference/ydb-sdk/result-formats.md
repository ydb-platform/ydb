# Формат данных в результате запроса

{{ ydb-short-name }} поддерживает несколько форматов представления данных и режимов возвращения схемы в результате выполнения запроса через QueryService. Формат и режим возвращения схемы задаются при выполнении запроса и распространяются на каждую инструкцию, которая возвращает данные (например, инструкция [SELECT](../../yql/reference/syntax/select/index.md) или ключевое слово `RETURNING`). Конфигурация этих настроек может отличаться между различными SDK.

## Форматы результатов {#result-format}

{% list tabs %}

- Value

  Формат по умолчанию. Данные возвращаются построчно: каждая строка представляет собой набор именованных значений с их YQL-типами. SDK автоматически преобразует полученные значения в нативные типы выбранного языка программирования, предоставляя типобезопасный интерфейс для работы с данными.

  Этот формат рекомендуется для:

  * Транзакционных ([OLTP](https://ru.wikipedia.org/wiki/OLTP)) задач, где требуется точечное чтение отдельных строк таблицы;
  * Приложений, которые обрабатывают небольшие фрагменты таблицы в полном размере;
  * Случаев, когда важна нативная интеграция с типами выбранного языка программирования;

  ### Конвертация YQL-типов в формате Value

  Конвертация YQL-типов выполняется в нативные типы языка программирования на стороне SDK и зависит от конкретной его реализации. Подробнее о маппинге типов смотрите в документации используемого SDK.

- Apache Arrow

  Данные возвращаются в колоночном формате [Apache Arrow](https://arrow.apache.org/) (стандарт [IPC](https://arrow.apache.org/docs/5.0/format/Columnar.html#serialization-and-interprocess-communication-ipc) версии 5.0) и не преобразовываются на стороне SDK, что позволяет эффективно обрабатывать большие объёмы данных.

  Этот формат рекомендуется для:

  * Аналитических ([OLAP](https://ru.wikipedia.org/wiki/OLAP)) задач, где данные обрабатываются колоночно — агрегации, фильтрации, сканирование по нескольким столбцам больших выборок;
  * Систем, которые нативно работают с Apache Arrow;
  * Задач, где важна высокая производительность при передаче больших объёмов данных.

  ### Конвертация YQL-типов в формате Arrow

  Типы данных YQL конвертируются в типы Apache Arrow по следующим правилам.

  #### Числовые типы

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

  #### Строковые типы

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `String` | `binary` | |
  | `Utf8` | `string` | |
  | `Json` | `string` | |
  | `JsonDocument` | `string` | Строковое представление бинарного [JSON](https://en.wikipedia.org/wiki/JSON) |
  | `Yson` | `binary` | |
  | `Uuid` | `fixed_size_binary(16)` | 16 байт в mixed-endian порядке |

  #### Временные типы

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Date` | `uint16` | Точность до дня |
  | `Date32` | `int32` | Точность до дня |
  | `Datetime` | `uint32` | Точность до секунды |
  | `Datetime64` | `int64` | Точность до секунды |
  | `Timestamp` | `uint64` | Точность до микросекунды |
  | `Timestamp64` | `int64` | Точность до микросекунды |
  | `Interval` | `int64` | Микросекунды |
  | `Interval64` | `int64` | Микросекунды |
  | `TzDate` | `struct<datetime: uint16, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzDate32` | `struct<datetime: int32, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzDatetime` | `struct<datetime: uint32, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzDatetime64` | `struct<datetime: int64, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzTimestamp` | `struct<datetime: uint64, timezone: string>` | Включает строковое имя метки таймзоны |
  | `TzTimestamp64` | `struct<datetime: int64, timezone: string>` | Включает строковое имя метки таймзоны |

  {% note info %}

  В формате Arrow базовые временные типы данных представляют собой беззнаковые целочисленные типы, в отличие от расширенных временных типов, которые являются знаковыми.

  {% endnote %}

  #### Сингулярные типы

  | Тип YQL | Тип Arrow |
  |---------|-----------|
  | `Null` | `null` |
  | `Void` | `struct<>` |
  | `EmptyList` | `struct<>` |
  | `EmptyDict` | `struct<>` |

  #### Составные типы

  | Тип YQL | Тип Arrow | Примечание |
  |---------|-----------|------------|
  | `Optional<T>` | `struct<opt: T>` | Если тип `T` является `Variant`, `Optional`, `Pg` или сингулярным |
  | `Optional<T>` | `T` | Для остальных типов, по умолчанию nullable в Apache Arrow |
  | `List<T>` | `list<item: T>` | |
  | `Tuple<T1, T2, ...>` | `struct<field0: T1, field1: T2, ...>` | |
  | `Struct<name: T, ...>` | `struct<name: T, ...>` | |
  | `Dict<K, V>` | `list<struct<key: K, payload: V>>` | |
  | `Variant<T1, ..., Tn>` | `dense_union<field0: T1, ...>` | Для n <= 128 |
  | `Variant<T1, ..., Tn>` | `dense_union<dense_union<field0: T1, ...>, ...>` | Для 128 < n <= 16384 |
  | `Tagged<T>` | `T` |  Раскрытие типа |

  {% note warning %}

  Тип `Variant` в YQL можно представить как тип Arrow, если количество перечисленных типов в `Variant` не превышает 16384 (128 * 128). В противном случае представление типа в этом формате не поддерживается.

  {% endnote %}

  #### Типы семейства pg

  Все типы семейства `pg` представляются типом Arrow `string` как текстовое представление значений.

  ### Сжатие данных в формате Arrow

  Для формата Apache Arrow можно настроить сжатие передаваемых данных. Доступны следующие кодеки:

  | Кодек | Описание |
  |-------|----------|
  | Без сжатия (по умолчанию) | |
  | `ZSTD` | Сжатие [Zstandard](https://github.com/facebook/zstd). Поддерживает настройку уровня сжатия |
  | `LZ4_FRAME` | Сжатие [LZ4](https://github.com/lz4/lz4). Настройка уровня сжатия не поддерживается |

{% endlist %}

## Режимы возвращения схемы {#schema-inclusion-mode}

При потоковом выполнении запроса результаты передаются частями. Режим возвращения схемы определяет, в каких частях потока содержится информация о схеме данных.

* **Always (по умолчанию)** — схема данных возвращается в каждой части потока результата. Удобен для простой обработки, когда каждая часть обрабатывается независимо.
* **First only** — схема данных возвращается только в первой части потока для каждого результирующего набора, в последующих частях она отсутствует. Позволяет сократить объём передаваемых метаданных при потоковой обработке больших результатов.

Состав возвращаемой схемы зависит от формата данных.

{% list tabs %}

- Value

  Схема содержит список столбцов с их YQL-типами. Эта информация достаточна для интерпретации значений на стороне SDK: зная тип столбца, SDK преобразует каждое значение в нативный тип языка программирования.

- Apache Arrow

  Схема содержит два компонента:

  * **Список столбцов с YQL-типами** — описывает исходные типы данных в терминах YQL. Позволяет приложению понять семантику данных независимо от особенностей Arrow-представления.
  * **Сериализованная схема Arrow RecordBatch** — описывает структуру полученных бинарных данных в терминах Apache Arrow. Необходима для корректной десериализации RecordBatch на стороне клиента.

  Наличие двух схем обусловлено тем, что YQL-типы и Arrow-типы не всегда имеют взаимно однозначное соответствие.

{% endlist %}

## Примеры использования в SDK {#sdk-examples}

{% list tabs group=lang %}

- Python

  ```python
  import pyarrow

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
