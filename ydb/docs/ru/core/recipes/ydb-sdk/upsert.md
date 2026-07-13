# Вставка данных

Ниже приведены примеры кода использования встроенных в {{ ydb-short-name }} SDK средств выполнения вставки:

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/query/client.h>

    void UpsertSeries(NYdb::NQuery::TQueryClient& client) {
      NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync(
          [](NYdb::NQuery::TSession session) {
              constexpr auto query = R"(
                  DECLARE $seriesData AS List<Struct<
                      series_id: Uint64,
                      title: Utf8,
                      series_info: Utf8,
                      comment: Optional<Utf8>
                  >>;

                  UPSERT INTO series
                  (
                      series_id,
                      title,
                      series_info,
                      comment
                  )
                  SELECT
                      series_id,
                      title,
                      series_info,
                      comment
                  FROM AS_TABLE($seriesData);
              )";

              auto params = NYdb::TParamsBuilder()
                  .AddParam("$seriesData")
                      .BeginList()
                      .AddListItem()
                          .BeginStruct()
                          .AddMember("series_id").Uint64(1)
                          .AddMember("title").Utf8("IT Crowd")
                          .AddMember("series_info").Utf8(
                              "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "
                              "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.")
                          .AddMember("comment").OptionalUtf8(std::nullopt)
                          .EndStruct()
                      .AddListItem()
                          .BeginStruct()
                          .AddMember("series_id").Uint64(2)
                          .AddMember("title").Utf8("Silicon Valley")
                          .AddMember("series_info").Utf8(
                              "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "
                              "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.")
                          .AddMember("comment").OptionalUtf8("lorem ipsum")
                          .EndStruct()
                      .EndList()
                      .Build()
                  .Build();

              return session.ExecuteQuery(
                  query,
                  NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx(),
                  params).GetValueSync();
          },
          NYdb::NQuery::TRetryOperationSettings()
              .Idempotent(true)
      ));
    }
    ```

  - userver

    ```cpp
    #include <userver/ydb/io/supported_types.hpp>
    #include <userver/ydb/table.hpp>

    struct SeriesData final {
        std::uint64_t series_id;
        ydb::Utf8 title;
        ydb::Utf8 series_info;
        std::optional<ydb::Utf8> comment;
    };

    void UpsertSeries(ydb::TableClient& client) {
        auto builder = client.GetBuilder();
        builder.Add(
            "$seriesData",
            std::vector<SeriesData>{
                {
                    .series_id = 1,
                    .title = ydb::Utf8{"IT Crowd"},
                    .series_info = ydb::Utf8{
                        "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "
                        "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."
                    },
                    .comment = std::nullopt,
                },
                {
                    .series_id = 2,
                    .title = ydb::Utf8{"Silicon Valley"},
                    .series_info = ydb::Utf8{
                        "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "
                        "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."
                    },
                    .comment = ydb::Utf8{"lorem ipsum"},
                },
            }
        );

        client.ExecuteQuery(
            ydb::OperationSettings{
                .tx_mode = ydb::TransactionMode::kSerializableRW,
                .is_idempotent = true,
            },
            ydb::Query{R"(
                DECLARE $seriesData AS List<Struct<
                    series_id: Uint64,
                    title: Utf8,
                    series_info: Utf8,
                    comment: Optional<Utf8>
                >>;

                UPSERT INTO series
                (
                    series_id,
                    title,
                    series_info,
                    comment
                )
                SELECT
                    series_id,
                    title,
                    series_info,
                    comment
                FROM AS_TABLE($seriesData);
            )"},
            std::move(builder)
        );
    }
    ```

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

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
      // execute upsert with native ydb data
      err = db.Table().DoTx( // Do retry operation on errors with best effort
        ctx, // context manages exiting from Do
        func(ctx context.Context, tx table.TransactionActor) (err error) { // retry operation
          res, err := tx.Execute(ctx, `
              PRAGMA TablePathPrefix("/path/to/table");
              DECLARE $seriesID AS Uint64;
              DECLARE $seasonID AS Uint64;
              DECLARE $episodeID AS Uint64;
              DECLARE $views AS Uint64;
              UPSERT INTO episodes ( series_id, season_id, episode_id, views )
              VALUES ( $seriesID, $seasonID, $episodeID, $views );
            `,
            table.NewQueryParameters(
              table.ValueParam("$seriesID", types.Uint64Value(1)),
              table.ValueParam("$seasonID", types.Uint64Value(1)),
              table.ValueParam("$episodeID", types.Uint64Value(1)),
              table.ValueParam("$views", types.Uint64Value(1)), // increment views
            ),
          )
          if err != nil {
            return err
          }
          if err = res.Err(); err != nil {
            return err
          }
          return res.Close()
        }, table.WithIdempotent(),
      )
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  - database/sql

    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      _ "github.com/ydb-platform/ydb-go-sdk/v3"
      "github.com/ydb-platform/ydb-go-sdk/v3/retry"
      "github.com/ydb-platform/ydb-go-sdk/v3/types"
    )

    func main() {
      db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      // execute upsert with native ydb data
      err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
        if _, err = tx.ExecContext(ctx,`
            PRAGMA TablePathPrefix("/local");
            REPLACE INTO series
            SELECT
              series_id,
              title,
              series_info,
              comment
            FROM AS_TABLE($seriesData);
          `,
          sql.Named("seriesData", types.ListValue(
            types.StructValue(
              types.StructFieldValue("series_id", types.Uint64Value(1)),
              types.StructFieldValue("title", types.TextValue("IT Crowd")),
              types.StructFieldValue("series_info", types.TextValue(
                "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
                "Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
              )),
              types.StructFieldValue("comment", types.NullValue(types.TypeText)),
            ),
            types.StructValue(
              types.StructFieldValue("series_id", types.Uint64Value(2)),
              types.StructFieldValue("title", types.TextValue("Silicon Valley")),
              types.StructFieldValue("series_info", types.TextValue(
                "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
                "Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
              )),
              types.StructFieldValue("comment", types.TextValue("lorem ipsum")),
            ),
          )),
        ); err != nil {
          return err
        }
        return nil
      }, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
      if err != nil {
        fmt.Printf("unexpected error: %v", err)
      }
    }
    ```

  {% endlist %}

- Java

  Операция [UPSERT INTO](../../yql/reference/syntax/upsert_into.md) вставляет строку или обновляет существующую по первичному ключу без предварительного чтения. Для массовой неатомарной загрузки см. [пакетную вставку](./bulk-upsert.md). Структура таблицы описана в разделе [Таблицы](../../concepts/datamodel/table.md).

  {% list tabs %}

  - Native SDK

    ```java
    import java.util.ArrayList;
    import java.util.List;

    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;
    import tech.ydb.table.values.ListType;
    import tech.ydb.table.values.ListValue;
    import tech.ydb.table.values.OptionalType;
    import tech.ydb.table.values.PrimitiveType;
    import tech.ydb.table.values.PrimitiveValue;
    import tech.ydb.table.values.StructType;
    import tech.ydb.table.values.Value;

    public class UpsertExample {
        public static void main(String[] args) {
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                // Создаём таблицу, если её ещё нет (DDL — без транзакции)
                retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery("""
                        CREATE TABLE IF NOT EXISTS series (
                            series_id Uint64,
                            title Text,
                            series_info Text,
                            comment Text,
                            PRIMARY KEY (series_id)
                        );
                        """, TxMode.NONE, Params.empty())
                )).join().getValue();

                // Подготовка данных для UPSERT через AS_TABLE($seriesData)
                StructType rowType = StructType.of(
                        "series_id", PrimitiveType.Uint64,
                        "title", PrimitiveType.Text,
                        "series_info", PrimitiveType.Text,
                        "comment", OptionalType.of(PrimitiveType.Text)
                );

                List<Value<?>> rows = new ArrayList<>();
                rows.add(rowType.newValue(
                        "series_id", PrimitiveValue.newUint64(1),
                        "title", PrimitiveValue.newText("IT Crowd"),
                        "series_info", PrimitiveValue.newText(
                                "The IT Crowd is a British sitcom produced by Channel 4..."),
                        "comment", OptionalType.of(PrimitiveType.Text).emptyValue()
                ));
                rows.add(rowType.newValue(
                        "series_id", PrimitiveValue.newUint64(2),
                        "title", PrimitiveValue.newText("Silicon Valley"),
                        "series_info", PrimitiveValue.newText(
                                "Silicon Valley is an American comedy television series..."),
                        "comment", OptionalType.of(PrimitiveType.Text).newValue(
                                PrimitiveValue.newText("lorem ipsum"))
                ));

                ListValue seriesData = ListType.of(rowType).newValue(rows);
                Params params = Params.of("$seriesData", seriesData);

                String upsertQuery = """
                        DECLARE $seriesData AS List<Struct<
                            series_id: Uint64,
                            title: Utf8,
                            series_info: Utf8,
                            comment: Optional<Utf8>
                        >>;
                        UPSERT INTO series (series_id, title, series_info, comment)
                        SELECT series_id, title, series_info, comment FROM AS_TABLE($seriesData);
                        """;

                retryCtx.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery(upsertQuery, TxMode.SERIALIZABLE_RW, params)
                )).join().getValue();

                // Проверка: должно быть 2 строки
                QueryReader countReader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery("SELECT COUNT(*) AS cnt FROM series", TxMode.NONE, Params.empty())
                )).join().getValue();

                ResultSetReader rs = countReader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("Строк в таблице series: " + rs.getColumn("cnt").getUint64());
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

    public class JdbcUpsertExample {
        public static void main(String[] args) throws SQLException {
            String url = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            try (Connection conn = DriverManager.getConnection(url)) {
                // Создаём таблицу (DDL выполняется в режиме автокоммита)
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("""
                            CREATE TABLE IF NOT EXISTS series (
                                series_id Uint64,
                                title Text,
                                series_info Text,
                                comment Text,
                                PRIMARY KEY (series_id)
                            );
                            """);
                }

                // REPLACE INTO — аналог UPSERT с полной перезаписью строки
                String replaceSql = """
                        REPLACE INTO series (series_id, title, series_info, comment)
                        VALUES (?, ?, ?, ?);
                        """;

                try (PreparedStatement ps = conn.prepareStatement(replaceSql)) {
                    ps.setLong(1, 1);
                    ps.setString(2, "IT Crowd");
                    ps.setString(3, "The IT Crowd is a British sitcom produced by Channel 4...");
                    ps.setNull(4, java.sql.Types.VARCHAR);
                    ps.executeUpdate();

                    ps.setLong(1, 2);
                    ps.setString(2, "Silicon Valley");
                    ps.setString(3, "Silicon Valley is an American comedy television series...");
                    ps.setString(4, "lorem ipsum");
                    ps.executeUpdate();
                }

                // Проверка количества строк
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM series")) {
                    if (rs.next()) {
                        System.out.println("Строк в таблице series: " + rs.getLong("cnt"));
                    }
                }
            }
        }
    }
    ```

    В Spring Boot, Hibernate, JOOQ и других фреймворках поверх JDBC драйвер также стремится оптимизировать **крупные** последовательности вставок и изменений: при необходимости **UPSERT**, `INSERT`, `UPDATE`, `DELETE` автоматически **группируются в пакеты** на стороне драйвера (в том числе при больших батчах из ORM).

  {% endlist %}

- Python

  {% list tabs %}

  - Native SDK

    Для вставки данных используется `QuerySessionPool` и метод `execute_with_retries` с параметризованным YQL-запросом. Запрос оперирует контейнерным типом `List<Struct<...>>`, что позволяет передавать несколько строк за один вызов.

    ```python
    import os
    import ydb

    with ydb.Driver(
        connection_string=os.environ["YDB_CONNECTION_STRING"],
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=5)
        pool = ydb.QuerySessionPool(driver)

        series_struct_type = ydb.StructType()
        series_struct_type.add_member("series_id", ydb.PrimitiveType.Uint64)
        series_struct_type.add_member("title", ydb.PrimitiveType.Utf8)
        series_struct_type.add_member("series_info", ydb.PrimitiveType.Utf8)
        series_struct_type.add_member("comment", ydb.OptionalType(ydb.PrimitiveType.Utf8))

        series_data = [
            {
                "series_id": 1,
                "title": "IT Crowd",
                "series_info": "The IT Crowd is a British sitcom...",
                "comment": None,
            },
            {
                "series_id": 2,
                "title": "Silicon Valley",
                "series_info": "Silicon Valley is an American comedy...",
                "comment": "lorem ipsum",
            },
        ]

        pool.execute_with_retries(
            """
            DECLARE $seriesData AS List<Struct<
                series_id: Uint64,
                title: Utf8,
                series_info: Utf8,
                comment: Optional<Utf8>
            >>;

            UPSERT INTO series
            (
                series_id,
                title,
                series_info,
                comment
            )
            SELECT
                series_id,
                title,
                series_info,
                comment
            FROM AS_TABLE($seriesData);
            """,
            {"$seriesData": (series_data, ydb.ListType(series_struct_type))},
            retry_settings=ydb.RetrySettings(idempotent=True),
        )
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb
    import asyncio

    async def ydb_init():
        async with ydb.aio.Driver(
            connection_string=os.environ["YDB_CONNECTION_STRING"],
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            await driver.wait()
            pool = ydb.aio.QuerySessionPool(driver)

            series_struct_type = ydb.StructType()
            series_struct_type.add_member("series_id", ydb.PrimitiveType.Uint64)
            series_struct_type.add_member("title", ydb.PrimitiveType.Utf8)
            series_struct_type.add_member("series_info", ydb.PrimitiveType.Utf8)
            series_struct_type.add_member("comment", ydb.OptionalType(ydb.PrimitiveType.Utf8))

            series_data = [
                {"series_id": 1, "title": "IT Crowd", "series_info": "The IT Crowd is a British sitcom...", "comment": None},
                {"series_id": 2, "title": "Silicon Valley", "series_info": "Silicon Valley is an American comedy...", "comment": "lorem ipsum"},
            ]

            await pool.execute_with_retries(
                """
                DECLARE $seriesData AS List<Struct<
                    series_id: Uint64,
                    title: Utf8,
                    series_info: Utf8,
                    comment: Optional<Utf8>
                >>;

                UPSERT INTO series (series_id, title, series_info, comment)
                SELECT series_id, title, series_info, comment FROM AS_TABLE($seriesData);
                """,
                {"$seriesData": (series_data, ydb.ListType(series_struct_type))},
                retry_settings=ydb.RetrySettings(idempotent=True),
            )

    asyncio.run(ydb_init())
    ```

  - SQLAlchemy

    При использовании {{ ydb-short-name }} через SQLAlchemy для вставки данных используется функция `ydb_sqlalchemy.upsert`, которая формирует запрос `UPSERT INTO` на основе таблицы и переданных значений. Можно вставлять как одну строку, так и несколько строк за один вызов:

    ```python
    import os
    import sqlalchemy as sa
    from sqlalchemy import Column, Integer, MetaData, String, Table
    import ydb_sqlalchemy as ydb_sa

    engine = sa.create_engine(os.environ["YDB_SQLALCHEMY_URL"])

    series = Table(
        "series",
        MetaData(),
        Column("series_id", Integer, primary_key=True),
        Column("title", String),
        Column("series_info", String),
        Column("comment", String, nullable=True),
    )

    with engine.connect() as connection:
        stmt = ydb_sa.upsert(series).values(
            [
                {
                    "series_id": 1,
                    "title": "IT Crowd",
                    "series_info": "The IT Crowd is a British sitcom...",
                    "comment": None,
                },
                {
                    "series_id": 2,
                    "title": "Silicon Valley",
                    "series_info": "Silicon Valley is an American comedy...",
                    "comment": "lorem ipsum",
                },
            ]
        )
        connection.execute(stmt)
        connection.commit()
    ```

  {% endlist %}

- C#

  ```C#
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Ado.YdbType;

  await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
  await using var connection = await dataSource.OpenRetryableConnectionAsync();

  var seriesData = new List<YdbStruct>
  {
      new()
      {
          { "series_id", 1UL, YdbDbType.Uint64 },
          { "title", "IT Crowd", YdbDbType.Text },
          { "series_info", "The IT Crowd is a British sitcom produced by Channel 4.", YdbDbType.Text },
          { "comment", null, YdbDbType.Text },
      },
      new()
      {
          { "series_id", 2UL, YdbDbType.Uint64 },
          { "title", "Silicon Valley", YdbDbType.Text },
          { "series_info", "Silicon Valley is an American comedy television series.", YdbDbType.Text },
          { "comment", "lorem ipsum", YdbDbType.Text },
      },
  };

  var command = new YdbCommand("UPSERT INTO series SELECT * FROM AS_TABLE($series_data)", connection);
  command.Parameters.Add(new YdbParameter("$series_data", seriesData));

  await command.ExecuteNonQueryAsync();
  ```

- JavaScript

  ```javascript
  import { Driver } from '@ydbjs/core'
  import { query } from '@ydbjs/query'

  const driver = new Driver('grpc://localhost:2136/local')
  await driver.ready()

  const users = [
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
  ]

  const sql = query(driver)
  await sql`UPSERT INTO users SELECT * FROM AS_TABLE(${users})`
  ```


- Rust

  ```rust
  use ydb::{
      ydb_params, ydb_struct, AccessTokenCredentials, ClientBuilder, Value, YdbResult,
  };

  fn series_row(
      series_id: u64,
      title: &str,
      series_info: &str,
      comment: Option<&str>,
  ) -> YdbResult<Value> {
      let comment_val = match comment {
          None => Value::optional_from(Value::Text(String::new()), None)?,
          Some(s) => Value::optional_from(
              Value::Text(String::new()),
              Some(Value::Text(s.into())),
          )?,
      };
      Ok(ydb_struct!(
          "series_id" => series_id,
          "title" => title,
          "series_info" => series_info,
          "comment" => comment_val,
      ))
  }

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string(
          "grpc://localhost:2136?database=local",
      )?
      .with_credentials(AccessTokenCredentials::from("..."))
      .client()?;

      client.wait().await?;

      let example = series_row(0, "", "", None)?;
      let series_data = Value::list_from(
          example,
          vec![
              series_row(
                  1,
                  "IT Crowd",
                  "The IT Crowd is a British sitcom...",
                  None,
              )?,
              series_row(
                  2,
                  "Silicon Valley",
                  "Silicon Valley is an American comedy...",
                  Some("lorem ipsum"),
              )?,
          ],
      )?;

      client
          .query_client()
          .exec(
              r#"
              PRAGMA TablePathPrefix("/local");
              UPSERT INTO series
              (
                  series_id,
                  title,
                  series_info,
                  comment
              )
              SELECT
                  series_id,
                  title,
                  series_info,
                  comment
              FROM AS_TABLE($seriesData);
              "#,
          )
          .params(ydb_params!("$seriesData" => series_data))
          .await?;

      Ok(())
  }
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Session;
  use YdbPlatform\Ydb\Ydb;

  $ydb = new Ydb($config);

  $yql = <<<'EOS'
  PRAGMA TablePathPrefix("/local");
  DECLARE $seriesData AS List<Struct<
      series_id: Uint64,
      title: Utf8,
      series_info: Utf8,
      comment: Optional<Utf8>
  >>;

  UPSERT INTO series
  (
      series_id,
      title,
      series_info,
      comment
  )
  SELECT
      series_id,
      title,
      series_info,
      comment
  FROM AS_TABLE($seriesData);
  EOS;

  $seriesData = [
      [
          'series_id' => 1,
          'title' => 'IT Crowd',
          'series_info' => 'The IT Crowd is a British sitcom...',
          'comment' => null,
      ],
      [
          'series_id' => 2,
          'title' => 'Silicon Valley',
          'series_info' => 'Silicon Valley is an American comedy...',
          'comment' => 'lorem ipsum',
      ],
  ];

  $ydb->table()->retryTransaction(
      function (Session $session) use ($yql, $seriesData) {
          return $session->prepare($yql)->execute([
              'seriesData' => $seriesData,
          ]);
      },
      true
  );
  ```

{% endlist %}
