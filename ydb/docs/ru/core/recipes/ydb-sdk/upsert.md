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

- Go (database/sql)

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
