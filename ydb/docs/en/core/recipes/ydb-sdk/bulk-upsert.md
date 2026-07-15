# Bulk upsert of data

{{ ydb-short-name }} supports bulk upsert of a large number of rows without atomicity guarantees. Data writes are split into multiple independent transactions, each affecting a single partition, and executed in parallel. This approach is therefore more efficient than `YQL`. On success, the `BulkUpsert` method guarantees insertion of all data passed in the request.

{% note warning %}

When using `BulkUpsert` to insert data into [columnar tables](../../concepts/datamodel/table.md#column-oriented-tables), you must provide values for **all** columns, including `NULL` values.

{% endnote %}

Below are code examples that use the built‑in {{ ydb-short-name }} SDK facilities for performing bulk upsert:

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/table/table.h>

    void BulkUpsertLogs(const NYdb::TDriver& driver) {
      NYdb::NTable::TTableClient client(driver);

      constexpr int kBatchSize = 1000;
      NYdb::TValueBuilder rowsBuilder;
      rowsBuilder.BeginList();
      for (int i = 0; i < kBatchSize; ++i) {
          rowsBuilder.AddListItem()
              .BeginStruct()
              .AddMember("App").Utf8("App_" + std::to_string(i / 256))
              .AddMember("Host").Utf8("192.168.0." + std::to_string(i % 256))
              .AddMember("Timestamp")
                  .Timestamp(TInstant::Now() + TDuration::Seconds(i))
              .AddMember("HttpCode").Uint32(static_cast<uint32_t>(i % 113 == 0 ? 404 : 200))
              .AddMember("Message")
                  .Utf8(i % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1")
              .EndStruct();
      }
      rowsBuilder.EndList();

      NYdb::TValue rows = rowsBuilder.Build();

      NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync(
          [&rows](NYdb::NTable::TTableClient& client) {
              return client.BulkUpsert("/local/bulk_upsert_example", NYdb::TValue{rows}).GetValueSync();
          },
          NYdb::NTable::TRetryOperationSettings()
              .Idempotent(true)
      ));
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/io/supported_types.hpp>
    #include <userver/ydb/table.hpp>

    struct LogMessage final {
        ydb::Utf8 App;
        ydb::Utf8 Host;
        std::chrono::system_clock::time_point Timestamp;
        std::uint32_t HttpCode;
        ydb::Utf8 Message;
    };

    void BulkUpsertLogs(ydb::TableClient& client) {
        constexpr int kBatchSize = 1000;
        std::vector<LogMessage> rows;
        rows.reserve(kBatchSize);

        for (int i = 0; i < kBatchSize; ++i) {
            rows.push_back({
                .App = ydb::Utf8{"App_" + std::to_string(i / 256)},
                .Host = ydb::Utf8{"192.168.0." + std::to_string(i % 256)},
                .Timestamp = std::chrono::system_clock::now() + std::chrono::seconds{i},
                .HttpCode = static_cast<std::uint32_t>(i % 113 == 0 ? 404 : 200),
                .Message = ydb::Utf8{
                    i % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1"
                },
            });
        }

        client.BulkUpsert(
            "/local/bulk_upsert_example",
            rows,
            ydb::OperationSettings{
                .is_idempotent = true,
            }
        );
    }
    ```

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

    {% cut "Batch upsert native {{ ydb-short-name }} data" %}

    ```go
    package main

    import (
      "context"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/table"
      "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      type logMessage struct {
        App       string
        Host      string
        Timestamp time.Time
        HTTPCode  uint32
        Message   string
      }
      // prepare native go data
      const batchSize = 10000
      logs := make([]logMessage, 0, batchSize)
      for i := 0; i < batchSize; i++ {
        message := logMessage{
          App:       fmt.Sprintf("App_%d", i/256),
          Host:      fmt.Sprintf("192.168.0.%d", i%256),
          Timestamp: time.Now().Add(time.Millisecond * time.Duration(i%1000)),
          HTTPCode:  200,
        }
        if i%2 == 0 {
          message.Message = "GET / HTTP/1.1"
        } else {
          message.Message = "GET /images/logo.png HTTP/1.1"
        }
        logs = append(logs, message)
      }
      // execute bulk upsert with native ydb data
      err = db.Table().Do( // Do retry operation on errors with best effort
        ctx, // context manage exiting from Do
        func(ctx context.Context, s table.Session) (err error) { // retry operation
          rows := make([]types.Value, 0, len(logs))
          for _, msg := range logs {
            rows = append(rows, types.StructValue(
              types.StructFieldValue("App", types.UTF8Value(msg.App)),
              types.StructFieldValue("Host", types.UTF8Value(msg.Host)),
              types.StructFieldValue("Timestamp", types.TimestampValueFromTime(msg.Timestamp)),
              types.StructFieldValue("HTTPCode", types.Uint32Value(msg.HTTPCode)),
              types.StructFieldValue("Message", types.UTF8Value(msg.Message)),
            ))
          }
          return s.BulkUpsert(ctx, "/local/bulk_upsert_example", types.ListValue(rows...))
        },
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

    {% endcut %}

    {% cut "Batch upsert `CSV`" %}

    ```go
    package main

    import (
      "context"
      "fmt"
      "os"

      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/table"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()

      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)

      csv := `skip row

    id,val
    42,"text42"
    43,"text43"
    44,hello
    `

      err = db.Table().BulkUpsert(ctx, "/local/bulk_upsert_example", table.BulkUpsertDataCsv(
        []byte(csv),
        table.WithCsvHeader(),
        table.WithCsvSkipRows(2),
        table.WithCsvNullValue([]byte("hello")), // string "hello" will be interpreted as NULL
      ))
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

    {% endcut %}

    {% cut "Batch upsert `Apache Arrow`" %}

    The following example uses the [arrow](https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow) package to prepare data.


    ```go
    package main

    import (
      "bytes"
      "context"
      "fmt"

      "github.com/apache/arrow-go/v18/arrow"
      "github.com/apache/arrow-go/v18/arrow/array"
      "github.com/apache/arrow-go/v18/arrow/ipc"
      "github.com/apache/arrow-go/v18/arrow/memory"
      "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/table"
    )

    func main() {
      ctx := context.Background()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx) // cleanup resources

      mem := memory.NewGoAllocator()

      schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64},
        {Name: "val", Type: arrow.BinaryTypes.String},
      }, nil)

      b := array.NewRecordBuilder(mem, schema)
      defer b.Release()

      b.Field(0).(*array.Int64Builder).AppendValues(
        []int64{123, 234}, nil)

      b.Field(1).(*array.StringBuilder).AppendValues(
        []string{"data1", "data2"}, nil)

      rec := b.NewRecordBatch()
      defer rec.Release()

      schemaPayload := ipc.GetSchemaPayload(rec.Schema(), mem)
      defer schemaPayload.Release()

      dataPayload, err := ipc.GetRecordBatchPayload(rec)
      if err != nil {
        panic(err)
      }
      defer dataPayload.Release()

      var schemaBuf bytes.Buffer
      _, err = schemaPayload.WritePayload(&schemaBuf)
      if err != nil {
        panic(err)
      }

      var dataBuf bytes.Buffer
      _, err = dataPayload.WritePayload(&dataBuf)
      if err != nil {
        panic(err)
      }

      err = db.Table().BulkUpsert(ctx, "/local/bulk_upsert_example", table.BulkUpsertDataArrow(
        dataBuf.Bytes(),
        table.WithArrowSchema(schemaBuf.Bytes()), // schema is required
      ))
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }

    ```

    {% endcut %}

  - database/sql

    The `database/sql` driver implementation for {{ ydb-short-name }} does not support non‑transactional bulk upsert.
    For bulk upsert you should use [transactional upsert](./upsert.md).

  {% endlist %}

- Java

  Bulk upsert via `BulkUpsert` is more efficient than transactional YQL for large data volumes. For small row sets see [UPSERT](./upsert.md). The table schema is described in the [Tables](../../concepts/datamodel/table.md) section.

  {% list tabs %}

  - Native SDK

    ```java
    import java.time.Instant;
    import java.util.ArrayList;
    import java.util.List;

    import tech.ydb.auth.NopAuthProvider;
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.TableClient;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.settings.BulkUpsertSettings;
    import tech.ydb.table.values.ListType;
    import tech.ydb.table.values.ListValue;
    import tech.ydb.table.values.PrimitiveType;
    import tech.ydb.table.values.PrimitiveValue;
    import tech.ydb.table.values.StructType;
    import tech.ydb.table.values.Value;

    public class BulkUpsertExample {
        private static final String TABLE_NAME = "bulk_upsert";
        private static final int BATCH_SIZE = 1000;

        public static void main(String[] args) {
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(NopAuthProvider.INSTANCE)
                    .build();
                 QueryClient queryClient = QueryClient.newClient(transport).build();
                 TableClient tableClient = TableClient.newClient(transport).build()) {

                SessionRetryContext queryRetry = SessionRetryContext.create(queryClient).build();
                SessionRetryContext tableRetry = SessionRetryContext.create(tableClient).build();

                // Creating a table for bulk upsert
                queryRetry.supplyResult(session -> QueryReader.readFrom(session.createQuery("""
                        CREATE TABLE IF NOT EXISTS bulk_upsert (
                            app Text,
                            timestamp Timestamp,
                            host Text,
                            http_code Uint32,
                            message Text,
                            PRIMARY KEY (app, timestamp, host)
                        );
                        """, TxMode.NONE, Params.empty())
                )).join().getValue();

                // Full path to the table: /local/bulk_upsert
                String tablePath = transport.getDatabase() + "/" + TABLE_NAME;

                StructType rowType = StructType.of(
                        "app", PrimitiveType.Text,
                        "timestamp", PrimitiveType.Timestamp,
                        "host", PrimitiveType.Text,
                        "http_code", PrimitiveType.Uint32,
                        "message", PrimitiveType.Text
                );

                // Generating a batch of records
                List<Value<?>> rows = new ArrayList<>(BATCH_SIZE);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    rows.add(rowType.newValue(
                            "app", PrimitiveValue.newText("App_" + i / 256),
                            "timestamp", PrimitiveValue.newTimestamp(Instant.now().plusSeconds(i)),
                            "host", PrimitiveValue.newText("192.168.0." + i % 256),
                            "http_code", PrimitiveValue.newUint32(i % 113 == 0 ? 404 : 200),
                            "message", PrimitiveValue.newText(
                                    i % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1")
                    ));
                }

                ListValue batch = ListType.of(rowType).newValue(rows);

                // Bulk upsert without atomicity of the entire batch
                tableRetry.supplyStatus(session ->
                        session.executeBulkUpsert(tablePath, batch, new BulkUpsertSettings())
                ).join().expectSuccess("bulk upsert failed");

                // Checking the row count
                QueryReader countReader = queryRetry.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery(
                                "SELECT COUNT(*) AS cnt FROM bulk_upsert", TxMode.NONE, Params.empty())
                )).join().getValue();

                ResultSetReader rs = countReader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("Строк в таблице bulk_upsert: " + rs.getColumn("cnt").getUint64());
                }
            }
        }
    }
    ```

  - JDBC

    ```java
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.PreparedStatement;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;
    import java.sql.Timestamp;
    import java.time.Instant;

    public class JdbcBulkUpsertExample {
        private static final int BATCH_SIZE = 1000;

        public static void main(String[] args) throws SQLException {
            String url = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            try (Connection conn = DriverManager.getConnection(url)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("""
                            CREATE TABLE IF NOT EXISTS bulk_upsert (
                                app Text,
                                timestamp Timestamp,
                                host Text,
                                http_code Uint32,
                                message Text,
                                PRIMARY KEY (app, timestamp, host)
                            );
                            """);
                }

                String bulkSql = """
                        BULK UPSERT INTO bulk_upsert (app, timestamp, host, http_code, message)
                        VALUES (?, ?, ?, ?, ?);
                        """;

                try (PreparedStatement ps = conn.prepareStatement(bulkSql)) {
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        ps.setString(1, "App_" + i / 256);
                        ps.setTimestamp(2, Timestamp.from(Instant.now().plusSeconds(i)));
                        ps.setString(3, "192.168.0." + i % 256);
                        ps.setLong(4, i % 113 == 0 ? 404 : 200);
                        ps.setString(5, i % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1");
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }

                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM bulk_upsert")) {
                    if (rs.next()) {
                        System.out.println("Строк в таблице bulk_upsert: " + rs.getLong("cnt"));
                    }
                }
            }
        }
    }
    ```


    In Spring Boot, Hibernate, JOOQ and other frameworks built on top of ORM over JDBC you can execute native YQL (including from repositories and `@Query`). The driver aims to optimize large inserts; operations `UPDATE`, `INSERT`, `DELETE`, `UPSERT` that go through JDBC are automatically grouped into batches on the driver side when needed.

  {% endlist %}

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import posixpath
    import ydb

    def bulk_upsert(driver: ydb.Driver, path: str):
        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("id", ydb.PrimitiveType.Uint64)
            .add_column("val", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        )
        rows = [
            {"id": 1, "val": "1"},
            {"id": 2, "val": "2"},
            {"id": 3, "val": "3"},
        ]
        driver.table_client.bulk_upsert(posixpath.join(path, "tablename"), rows, column_types)
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import posixpath
    import ydb
    import asyncio

    async def bulk_upsert(driver: ydb.aio.Driver, path: str):
        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("id", ydb.PrimitiveType.Uint64)
            .add_column("val", ydb.OptionalType(ydb.PrimitiveType.Utf8))
        )
        rows = [
            {"id": 1, "val": "1"},
            {"id": 2, "val": "2"},
            {"id": 3, "val": "3"},
        ]
        await driver.table_client.bulk_upsert(
            posixpath.join(path, "tablename"), rows, column_types
        )

    async def main():
        async with ydb.aio.Driver(
            connection_string=os.environ["YDB_CONNECTION_STRING"],
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            await driver.wait()
            await bulk_upsert(driver, "/local")

    asyncio.run(main())
    ```

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa
    import ydb

    engine = sa.create_engine(os.environ["YDB_SQLALCHEMY_URL"])
    with engine.connect() as connection:
        dbapi_conn = connection.connection

        column_types = (
              ydb.BulkUpsertColumns()
              .add_column("id", ydb.PrimitiveType.Uint64)
              .add_column("val", ydb.OptionalType(ydb.PrimitiveType.Utf8))
          )
        rows = [
            {"id": 1, "val": "1"},
            {"id": 2, "val": "2"},
            {"id": 3, "val": "3"},
        ]

        dbapi_conn.bulk_upsert("tablename", rows, column_types)
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  ```rust
  use ydb::{ydb_struct, AccessTokenCredentials, ClientBuilder, Value, YdbResult};

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string(
          "grpc://localhost:2136?database=local",
      )?
      .with_credentials(AccessTokenCredentials::from("..."))
      .client()?;

      client.wait().await?;

      let rows: Vec<Value> = vec![
          ydb_struct!(
              "id" => 1_u64,
              "val" => Value::Text("1".into()),
          ),
          ydb_struct!(
              "id" => 2_u64,
              "val" => Value::Text("2".into()),
          ),
          ydb_struct!(
              "id" => 3_u64,
              "val" => Value::Text("3".into()),
          ),
      ];

      client
          .table_client()
          .bulk_upsert("/local/tablename", rows)
          .idempotent(true)
          .await?;

      Ok(())
  }
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $ydb = new Ydb($config);

  $rows = [
      ['id' => 1, 'val' => '1'],
      ['id' => 2, 'val' => '2'],
      ['id' => 3, 'val' => '3'],
  ];

  $ydb->table()->bulkUpsert('tablename', $rows, [
      'id'  => 'UINT64',
      'val' => 'UTF8',
  ]);
  ```

{% endlist %}
