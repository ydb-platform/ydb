# Формат Apache Arrow

Данные возвращаются в колоночном формате [Apache Arrow](https://arrow.apache.org/) (стандарт [IPC](https://arrow.apache.org/docs/5.0/format/Columnar.html#serialization-and-interprocess-communication-ipc) версии 5.0) и не преобразуются на стороне SDK, что позволяет эффективно обрабатывать большие объёмы данных.

Этот формат рекомендуется для:

* Аналитических ([OLAP](https://ru.wikipedia.org/wiki/OLAP)) задач, где данные обрабатываются колоночно — агрегации, фильтрации, сканирование по нескольким столбцам больших выборок;
* Систем, которые нативно работают с Apache Arrow;
* Задач, где важна высокая производительность при передаче больших объёмов данных.

## Конвертация YQL-типов {#type-mapping}

[Типы данных YQL](../../../yql/reference/types/index.md) конвертируются в типы Apache Arrow по следующим правилам.

### Числовые типы

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

### Строковые типы

| Тип YQL | Тип Arrow | Примечание |
|---------|-----------|------------|
| `String` | `binary` | |
| `Utf8` | `string` | |
| `Json` | `string` | |
| `JsonDocument` | `string` | Строковое представление бинарного [JSON](https://en.wikipedia.org/wiki/JSON) |
| `Yson` | `binary` | |
| `Uuid` | `fixed_size_binary(16)` | 16 байт в mixed-endian порядке |

### Временные типы

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

### Контейнерные типы

| Тип YQL | Тип Arrow | Примечание |
|---------|-----------|------------|
| `List<T>` | `list<item: T>` | |
| `Tuple<T1, T2, ...>` | `struct<field0: T1, field1: T2, ...>` | |
| `Struct<name: T, ...>` | `struct<name: T, ...>` | |
| `Dict<K, V>` | `list<struct<key: K, payload: V>>` | |
| `Set<T>` | `list<struct<key: T, payload: struct<>>>` | Является `Dict<T, Void>` |
| `Variant<T1, ..., Tn>` | `dense_union<field0: T1, ...>` | Для n <= 128 |
| `Variant<T1, ..., Tn>` | `dense_union<dense_union<field0: T1, ...>, ...>` | Для 128 < n <= 16384 |
| `Variant<name1: T1, ..., nameN: Tn>` | `dense_union<name1: T1, ...>` | Для n <= 128 |
| `Variant<name1: T1, ..., nameN: Tn>` | `dense_union<dense_union<name1: T1, ...>, ...>` | Для 128 < n <= 16384 |

{% note warning %}

Типы `Variant` над кортежем и над структурой не представимы в формате Apache Arrow, если количество дочерних типов превышает 16384 (128 * 128).

{% endnote %}

### Опциональные и специальные типы

| Тип YQL | Тип Arrow | Примечание |
|---------|-----------|------------|
| `Null` | `null` | Сингулярный тип |
| `Void` | `struct<>` | Сингулярный тип |
| `EmptyList` | `struct<>` | Сингулярный тип |
| `EmptyDict` | `struct<>` | Сингулярный тип |
| `Tagged<T>` | `T` | Раскрытие с потерей именования |
| `Optional<T>` | `struct<opt: T>` | Если тип `T` является `Variant`, `Optional`, `Pg` или сингулярным |
| `Optional<T>` | `T` | Для остальных типов |

### Типы семейства pg

Все типы семейства `pg` представляются типом Arrow `string` как текстовое представление значений.

## Сжатие данных {#compression}

Для формата Apache Arrow можно настроить сжатие передаваемых данных. Доступны следующие кодеки:

| Кодек | Описание |
|-------|----------|
| Без сжатия (по умолчанию) | |
| `ZSTD` | Сжатие [Zstandard](https://github.com/facebook/zstd). Поддерживает настройку уровня сжатия |
| `LZ4_FRAME` | Сжатие [LZ4](https://github.com/lz4/lz4). Настройка уровня сжатия не поддерживается |

## Схема возвращаемых данных {#schema}

Схема содержит два компонента:

* **Список столбцов с YQL-типами** — описывает исходные типы данных в терминах YQL. Позволяет приложению понять семантику данных независимо от особенностей Arrow-представления.
* **Сериализованная схема Arrow RecordBatch** — описывает структуру полученных бинарных данных в терминах Apache Arrow. Необходима для корректной десериализации RecordBatch на стороне клиента.

Наличие двух схем обусловлено тем, что YQL-типы и Arrow-типы не всегда имеют взаимно однозначное соответствие.

## Примеры использования Apache Arrow в SDK {#sdk-examples}

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
  String query = "SELECT * FROM example ORDER BY Key LIMIT 100;";

  ExecuteQuerySettings settings = ExecuteQuerySettings.newBuilder()
          .useApacheArrowFormat(ApacheArrowFormat.zstd())
          .build();

  try (RootAllocator allocator = new RootAllocator()) {
      retryCtx.supplyResult(session -> session
              .createQuery(query, TxMode.SERIALIZABLE_RW, Params.empty(), settings)
              .execute(new ApacheArrowCompressedPartsHandler(allocator) {
                  @Override
                  public void onNextPart(QueryResultPart part) {
                      ResultSetReader rs = part.getResultSetReader();
                      while (rs.next()) {
                          String key = rs.getColumn("Key").getText();
                          System.out.println("Read row with key " + key);
                      }
                      System.out.printf("Record batch with %d rows and %d columns%n",
                              rs.getRowCount(), rs.getColumnCount());
                  }
              })
      ).join().getStatus().expectSuccess("execute query problem");
  }
  ```

- C#

  {% include [feature-not-supported](../../../_includes/feature-not-supported.md) %}

{% endlist %}
