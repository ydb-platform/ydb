# Пакетная вставка данных

{{ ydb-short-name }} поддерживает пакетную вставку большого количества строк без гарантий атомарности. Запись данных разбивается на несколько независимых транзакций, каждая их которых затрагивает единственную партицию, с параллельным исполнением. За счет этого такой подход более эффективен чем `YQL`. В случае успеха метод `BulkUpsert` гарантирует вставку всех данных, переданных в рамках данного запроса.

Ниже приведены примеры кода использования встроенных в {{ ydb-short-name }} SDK средств выполнения пакетной вставки:

{% list tabs %}

- Go (native)

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

- Go (database/sql)

  Реализация `database/sql` драйвера для {{ ydb-short-name }} не поддерживает нетранзакционную пакетную вставку данных.
  Для пакетной вставки следует пользоваться [транзакционной вставкой](./upsert.md).

- Java

  ```java
    private static final String TABLE_NAME = "bulk_upsert";
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        String connectionString = args[0];

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(NopAuthProvider.INSTANCE) // анонимная аутентификация 
                .build()) {

            // Для bulk upsert необходимо использовать полный путь к таблице
            String tablePath = transport.getDatabase() + "/" + TABLE_NAME;
            try (TableClient tableClient = TableClient.newClient(transport).build()) {
                SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
                execute(retryCtx, tablePath);
            }
        }
    }

    public static void execute(SessionRetryContext retryCtx, String tablePath) {
        // описание таблицы
        StructType structType = StructType.of(
            "app", PrimitiveType.Text,
            "timestamp", PrimitiveType.Timestamp,
            "host", PrimitiveType.Text,
            "http_code", PrimitiveType.Uint32,
            "message", PrimitiveType.Text
        );

        // генерация пакета записей
        List<Value<?>> list = new ArrayList<>(50);
        for (int i = 0; i < BATCH_SIZE; i += 1) {
            // добавление новой строки в виде значения-структуры
            list.add(structType.newValue(
                "app", PrimitiveValue.newText("App_" + String.valueOf(i / 256)),
                "timestamp", PrimitiveValue.newTimestamp(Instant.now().plusSeconds(i)),
                "host", PrimitiveValue.newText("192.168.0." + i % 256),
                "http_code", PrimitiveValue.newUint32(i % 113 == 0 ? 404 : 200),
                "message", PrimitiveValue.newText(i % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1")
            ));
        }

        // Create list of structs
        ListValue rows = ListType.of(structType).newValue(list);
        // Do retry operation on errors with best effort
        retryCtx.supplyStatus(
            session -> session.executeBulkUpsert(tablePath, rows, new BulkUpsertSettings())
        ).join().expectSuccess("bulk upsert problem");
    }
  ```

- JDBC

  ```java
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        String connectionUrl = args[0];

        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "BULK UPSERT INTO bulk_upsert (app, timestamp, host, http_code, message) VALUES (?, ?, ?, ?, ?);"
            )) {
                for (int i = 0; i < BATCH_SIZE; i += 1) {
                    ps.setString(1, "App_" + String.valueOf(i / 256));
                    ps.setTimestamp(2, Timestamp.from(Instant.now().plusSeconds(i)));
                    ps.setString(3, "192.168.0." + i % 256);
                    ps.setLong(4,i % 113 == 0 ? 404 : 200);
                    ps.setString(5, i % 3 == 0 ? "GET / HTTP/1.1" : "GET /images/logo.png HTTP/1.1");
                    ps.addBatch();
                }

                ps.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
<<<<<<< HEAD
=======
    ```

    В Spring Boot, Hibernate, JOOQ и других фреймворках вокруг ORM поверх JDBC можно выполнять нативный YQL (в том числе из репозиториев и `@Query`). Драйвер стремится оптимизировать крупные вставки; операции `UPDATE`, `INSERT`, `DELETE`, `UPSERT`, идущие через JDBC, при необходимости автоматически группируются в пакеты на стороне драйвера.

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
          .retry_execute_bulk_upsert("/local/tablename".into(), rows)
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
>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))
  ```

{% endlist %}
