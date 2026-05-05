# Apache Arrow format

Results are returned in the columnar [Apache Arrow](https://arrow.apache.org/) format ([IPC](https://arrow.apache.org/docs/5.0/format/Columnar.html#serialization-and-interprocess-communication-ipc) standard, version 5.0) without conversion inside the SDK, which makes large result sets efficient to process.

Prefer this format for:

* Analytical ([OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing)) workloads with columnar processing — aggregations, filters, scans over multiple columns on large samples.
* Systems that integrate natively with Apache Arrow.
* Scenarios where throughput for large data transfers matters most.

## YQL type mapping {#type-mapping}

[YQL data types](../../../yql/reference/types/index.md) are mapped to Apache Arrow types as follows.

### Numeric types

| YQL type | Arrow type | Notes |
|----------|------------|-------|
| `Bool` | `uint8` | `true`/`false` encoded as `1`/`0` |
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
| `Decimal(p, s)` | `fixed_size_binary(16)` | 120 bits used; ±inf and NaN markers supported |
| `DyNumber` | `string` | String representation of the number |

### String types

| YQL type | Arrow type | Notes |
|----------|------------|-------|
| `String` | `binary` | |
| `Utf8` | `string` | |
| `Json` | `string` | |
| `JsonDocument` | `string` | String form of binary [JSON](https://en.wikipedia.org/wiki/JSON) |
| `Yson` | `binary` | |
| `Uuid` | `fixed_size_binary(16)` | 16 bytes in mixed-endian order |

### Date and time types

| YQL type | Arrow type | Notes |
|----------|------------|-------|
| `Date` | `uint16` | Day precision |
| `Date32` | `int32` | Day precision |
| `Datetime` | `uint32` | Second precision |
| `Datetime64` | `int64` | Second precision |
| `Timestamp` | `uint64` | Microsecond precision |
| `Timestamp64` | `int64` | Microsecond precision |
| `Interval` | `int64` | Microseconds |
| `Interval64` | `int64` | Microseconds |
| `TzDate` | `struct<datetime: uint16, timezone: string>` | Time zone name as a string |
| `TzDate32` | `struct<datetime: int32, timezone: string>` | Time zone name as a string |
| `TzDatetime` | `struct<datetime: uint32, timezone: string>` | Time zone name as a string |
| `TzDatetime64` | `struct<datetime: int64, timezone: string>` | Time zone name as a string |
| `TzTimestamp` | `struct<datetime: uint64, timezone: string>` | Time zone name as a string |
| `TzTimestamp64` | `struct<datetime: int64, timezone: string>` | Time zone name as a string |

{% note info %}

In Arrow, basic date/time types are represented as unsigned integers, while extended date/time types types use signed integers.

{% endnote %}

### Container types

| YQL type | Arrow type | Notes |
|----------|------------|-------|
| `List<T>` | `list<item: T>` | |
| `Tuple<T1, T2, ...>` | `struct<field0: T1, field1: T2, ...>` | |
| `Struct<name: T, ...>` | `struct<name: T, ...>` | |
| `Dict<K, V>` | `list<struct<key: K, payload: V>>` | |
| `Set<T>` | `list<struct<key: T, payload: struct<>>>` | Same as `Dict<T, Void>` |
| `Variant<T1, ..., Tn>` | `dense_union<field0: T1, ...>` | For n <= 128 |
| `Variant<T1, ..., Tn>` | `dense_union<dense_union<field0: T1, ...>, ...>` | For 128 < n <= 16384 |
| `Variant<name1: T1, ..., nameN: Tn>` | `dense_union<name1: T1, ...>` | For n <= 128 |
| `Variant<name1: T1, ..., nameN: Tn>` | `dense_union<dense_union<name1: T1, ...>, ...>` | For 128 < n <= 16384 |

{% note warning %}

`Variant` over tuple or struct cannot be represented in Apache Arrow if the number of child types exceeds 16384 (128 * 128).

{% endnote %}

### Optional and special types

| YQL type | Arrow type | Notes |
|----------|------------|-------|
| `Null` | `null` | Singular type |
| `Void` | `struct<>` | Singular type |
| `EmptyList` | `struct<>` | Singular type |
| `EmptyDict` | `struct<>` | Singular type |
| `Tagged<T>` | `T` | Tag stripped |
| `Optional<T>` | `struct<opt: T>` | When `T` is `Variant`, `Optional`, `Pg`, or singular |
| `Optional<T>` | `T` | For all other `T` |

### Types in the `pg` family

All `pg` types are represented as Arrow `string` values containing the textual form.

## Data compression {#compression}

You can enable compression for Arrow payloads. Supported codecs:

| Codec | Description |
|-------|-------------|
| None (default) | |
| `ZSTD` | [Zstandard](https://github.com/facebook/zstd). Compression level is configurable |
| `LZ4_FRAME` | [LZ4](https://github.com/lz4/lz4). Compression level is not configurable |

## Returned schema {#schema}

The schema has two parts:

* **Column list with YQL types** — describes the logical types in YQL terms so your application can interpret semantics regardless of Arrow representation details.
* **Serialized Arrow RecordBatch schema** — describes the binary layout in Apache Arrow terms. Required to deserialize RecordBatch on the client.

Both are needed because YQL and Arrow types do not always map one-to-one.

## Apache Arrow examples in the SDK {#sdk-examples}

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

{% endlist %}
