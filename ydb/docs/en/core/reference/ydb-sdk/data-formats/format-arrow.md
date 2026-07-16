# Apache Arrow format

Data is returned in the columnar [Apache Arrow](https://arrow.apache.org/) format ([IPC](https://arrow.apache.org/docs/5.0/format/Columnar.html#serialization-and-interprocess-communication-ipc) standard version 5.0) and is not transformed on the SDK side, which allows efficient processing of large data volumes.

This format is recommended for:

* Analytical ( [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing)) tasks where data is processed column-wise — aggregations, filtering, scanning across multiple columns of large datasets;
* Systems that natively work with Apache Arrow;
* Tasks where high performance is important when transferring large data volumes.

## YQL type conversion {#type-mapping}

[YQL data types](../../../yql/reference/types/index.md) are converted to Apache Arrow types according to the following rules.

### Numeric types

| YQL type | Arrow type | Note |
| --- | --- | --- |
| `Bool` | `uint8` | Values `true`/`false` are encoded as `1`/`0` |
| `Int8` | `int8` |  |
| `Int16` | `int16` |  |
| `Int32` | `int32` |  |
| `Int64` | `int64` |  |
| `Uint8` | `uint8` |  |
| `Uint16` | `uint16` |  |
| `Uint32` | `uint32` |  |
| `Uint64` | `uint64` |  |
| `Float` | `float` |  |
| `Double` | `double` |  |
| `Decimal(p, s)` | `fixed_size_binary(16)` | Uses 120 bits, has ±inf and NaN markers |
| `DyNumber` | `string` | String representation of the number |

### String types

| YQL type | Arrow type | Note |
| --- | --- | --- |
| `String` | `binary` |  |
| `Utf8` | `string` |  |
| `Json` | `string` |  |
| `JsonDocument` | `string` | String representation of binary [JSON](https://en.wikipedia.org/wiki/JSON) |
| `Yson` | `binary` |  |
| `Uuid` | `fixed_size_binary(16)` | 16 bytes in mixed-endian order |

### Temporal types

| YQL type | Arrow type | Note |
| --- | --- | --- |
| `Date` | `uint16` | Day precision |
| `Date32` | `int32` | Day precision |
| `Datetime` | `uint32` | Second precision |
| `Datetime64` | `int64` | Second precision |
| `Timestamp` | `uint64` | Microsecond precision |
| `Timestamp64` | `int64` | Microsecond precision |
| `Interval` | `int64` | Microseconds |
| `Interval64` | `int64` | Microseconds |
| `TzDate` | `struct<datetime: uint16, timezone: string>` | Includes the string timezone label name |
| `TzDate32` | `struct<datetime: int32, timezone: string>` | Includes the string timezone label name |
| `TzDatetime` | `struct<datetime: uint32, timezone: string>` | Includes the string timezone label name |
| `TzDatetime64` | `struct<datetime: int64, timezone: string>` | Includes the string timezone label name |
| `TzTimestamp` | `struct<datetime: uint64, timezone: string>` | Includes the string timezone label name |
| `TzTimestamp64` | `struct<datetime: int64, timezone: string>` | Includes the string timezone label name |

{% note info %}

In the Arrow format, basic temporal data types are unsigned integer types, unlike extended temporal types, which are signed.

{% endnote %}

### Container types

| YQL type | Arrow type | Note |
| --- | --- | --- |
| `List<T>` | `list<item: T>` |  |
| `Tuple<T1, T2, ...>` | `struct<field0: T1, field1: T2, ...>` |  |
| `Struct<name: T, ...>` | `struct<name: T, ...>` |  |
| `Dict<K, V>` | `list<struct<key: K, payload: V>>` |  |
| `Set<T>` | `list<struct<key: T, payload: struct<>>>` | Is `Dict<T, Void>` |
| `Variant<T1, ..., Tn>` | `dense_union<field0: T1, ...>` | For n <= 128 |
| `Variant<T1, ..., Tn>` | `dense_union<dense_union<field0: T1, ...>, ...>` | For 128 < n <= 16384 |
| `Variant<name1: T1, ..., nameN: Tn>` | `dense_union<name1: T1, ...>` | For n <= 128 |
| `Variant<name1: T1, ..., nameN: Tn>` | `dense_union<dense_union<name1: T1, ...>, ...>` | For 128 < n <= 16384 |

{% note warning %}

`Variant` types over a tuple and over a struct are not representable in the Apache Arrow format if the number of child types exceeds 16384 (128 * 128).

{% endnote %}

### Optional and special types

| YQL type | Arrow type | Note |
| --- | --- | --- |
| `Null` | `null` | Singular type |
| `Void` | `struct<>` | Singular type |
| `EmptyList` | `struct<>` | Singular type |
| `EmptyDict` | `struct<>` | Singular type |
| `Tagged<T>` | `T` | Unfolding with loss of naming |
| `Optional<T>` | `struct<opt: T>` | If the `T` type is `Variant`, `Optional`, `Pg`, or singular |
| `Optional<T>` | `T` | For other types |

### pg family types

All types of the `pg` family are represented by the Arrow type `string` as a text representation of values.

## Data compression {#compression}

For the Apache Arrow format, you can configure compression of transmitted data. The following codecs are available:

| Codec | Description |
| --- | --- |
| No compression (default) |  |
| `ZSTD` | [Zstandard](https://github.com/facebook/zstd) compression. Supports compression level setting |
| `LZ4_FRAME` | [LZ4](https://github.com/lz4/lz4) compression. Compression level setting is not supported |

## Returned data schema {#schema}

The schema contains two components:

* **List of columns with YQL types** — describes the original data types in YQL terms. Allows the application to understand the semantics of the data regardless of the specifics of the Arrow representation.
* **Serialized Arrow RecordBatch schema** — describes the structure of the received binary data in Apache Arrow terms. Required for correct deserialization of RecordBatch on the client side.

The presence of two schemas is due to the fact that YQL types and Arrow types do not always have a one-to-one correspondence.

## Examples of using Apache Arrow in the SDK {#sdk-examples}

{% list tabs group=lang %}

- C++

  In the query execution settings, the result format `Arrow`, the schema inclusion mode `FirstOnly`, and (similar to the Python example) ZSTD compression with level 10 are specified. The serialized IPC schema and binary RecordBatch are read through [`NYdb::TArrowAccessor`](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/arrow/accessor.h) — this API is marked as experimental; further deserialization is performed using [Apache Arrow C++](https://arrow.apache.org/docs/cpp/). The session `NYdb::NQuery::TSession` is usually obtained in the callback `TQueryClient::RetryQuerySync` / `RetryQuery`.


  ```cpp
  #include <ydb-cpp-sdk/client/arrow/accessor.h>
  #include <ydb-cpp-sdk/client/query/client.h>

  NYdb::TStatus ExampleArrow(NYdb::NQuery::TSession session) {
      constexpr std::string_view query = "SELECT * FROM example ORDER BY Key LIMIT 100;";

      auto settings = NYdb::NQuery::TExecuteQuerySettings()
          .Format(NYdb::TResultSet::EFormat::Arrow)
          .SchemaInclusionMode(NYdb::NQuery::ESchemaInclusionMode::FirstOnly)
          .ArrowFormatSettings(NYdb::NQuery::TArrowFormatSettings()
              .CompressionCodec(NYdb::NQuery::TArrowFormatSettings::TCompressionCodec()
                  .Type(NYdb::NQuery::TArrowFormatSettings::TCompressionCodec::EType::Zstd)
                  .Level(10)
              )
          );

      auto queryResult = session.ExecuteQuery(
          query,
          NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
          settings).GetValueSync();

      NYdb::NStatusHelpers::ThrowOnError(queryResult);

      for (const NYdb::TResultSet& resultSet : queryResult.GetResultSets()) {
          const std::string& schema = NYdb::TArrowAccessor::GetArrowSchema(resultSet);
          const std::vector<std::string>& batches = NYdb::TArrowAccessor::GetArrowBatches(resultSet);
          std::cout << "Arrow schema size: " << schema.size() << ", batches: " << batches.size()
                    << std::endl;
      }
  }
  ```

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
