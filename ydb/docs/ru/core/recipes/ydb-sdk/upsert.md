# Вставка данных

Ниже приведены примеры кода использования встроенных в {{ ydb-short-name }} SDK средств выполнения вставки:

{% list tabs %}

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

  {% list tabs %}

  - Native SDK

    Используйте `SessionRetryContext` и `TableSession.executeDataQuery` с параметром `$seriesData` типа `List<Struct<...>>`. Значения для `AS_TABLE($seriesData)` собираются так же, как структуры строк в примере [пакетной вставки](./bulk-upsert.md).

    ```java
    SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();

    String yql = """
            PRAGMA TablePathPrefix("/local");
            DECLARE $seriesData AS List<Struct<
                series_id: Uint64,
                title: Utf8,
                series_info: Utf8,
                comment: Optional<Utf8>
            >>;
            UPSERT INTO series
            SELECT series_id, title, series_info, comment FROM AS_TABLE($seriesData);
            """;

    Params params = Params.of("$seriesData", seriesDataListValue);

    retryCtx.supplyResult(session -> session.executeDataQuery(yql, TxControl.serializableRw(), params))
            .join();
    ```

  - JDBC

    ```java
    try (Connection conn = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local");
         PreparedStatement ps = conn.prepareStatement(
                 """
                 REPLACE INTO series (series_id, title, series_info, comment)
                 SELECT series_id, title, series_info, comment FROM AS_TABLE($seriesData);
                 """
         )) {
        // Параметр $seriesData задаётся в соответствии с типом запроса (см. документацию JDBC-драйвера)
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
      ydb_params, ydb_struct, AccessTokenCredentials, ClientBuilder, Query, Value, YdbResult,
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

      let query = Query::new(
          r#"
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
          "#,
      )
      .with_params(ydb_params!("$seriesData" => series_data));

      client
          .table_client()
          .retry_transaction(|mut t| {
              let query = query.clone();
              async move {
                  t.query(query).await?;
                  t.commit().await?;
                  Ok(())
              }
          })
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
