# Retrying

{{ ydb-short-name }} is a distributed DBMS with automatic scaling under load.
Maintenance work may be performed on the server side, server racks or entire data centers may be temporarily disconnected.
Therefore, some errors are allowed when working with {{ ydb-short-name }}.
Depending on the error type, you should react differently to them.
{{ ydb-short-name }} SDKs provide built-in retry mechanisms for high availability,
which take into account error types and have a fixed reaction to them.

Below are code examples of using the built-in retry mechanisms of the {{ ydb-short-name }} SDK:

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    In the {{ ydb-short-name }} C++ SDK, retry with proper error handling is implemented in several APIs:

    {% cut "Synchronous execution retry attempts" %}

    To execute queries with automatic retries, the `RetryQuerySync` method is used.
    The method takes a lambda function that receives a session object and returns the query result.
    The {{ ydb-short-name }} C++ SDK automatically analyzes errors and performs retries according to their type.

    Example code using `RetryQuerySync`:


    ```c++
    #include <ydb-cpp-sdk/client/query/client.h>

    void ExecuteQueryWithRetry(NYdb::NQuery::TQueryClient client) {
        auto result = client.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
            auto query = R"(
                SELECT series_id, title
                FROM series
                WHERE series_id = 1;
            )";

            auto result = session.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()
            ).GetValueSync();

            if (!result.IsSuccess()) {
                return result;
            }

            // Processing the query result
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                std::cout << "Series"
                    << ", Id: " << parser.ColumnParser("series_id").GetOptionalUint64().value()
                    << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8().value()
                    << std::endl;
            }

            return result;
        });

        if (!result.IsSuccess()) {
            // Error handling after all attempts
            std::cerr << "Query failed: " << result.GetIssues().ToString() << std::endl;
        }
    }
    ```

    {% endcut %}

    {% cut "Asynchronous execution retry attempts" %}

    For asynchronous query execution with automatic retries, the `RetryQuery` method is used.
    The method returns `NThreading::TFuture`, which allows operations to be performed asynchronously.

    Example code using `RetryQuery`:


    ```c++
    #include <ydb-cpp-sdk/client/query/client.h>

    void ExecuteQueryWithRetryAsync(NYdb::NQuery::TQueryClient client) {
        auto future = client.RetryQuery([](NYdb::NQuery::TSession session) -> NYdb::TAsyncStatus {
            auto query = R"(
                SELECT series_id, title, release_date
                FROM series
                WHERE series_id = 1;
            )";

            return session.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()
            ).Apply([](const NYdb::NQuery::TAsyncExecuteQueryResult& asyncResult) -> NYdb::TStatus {
                auto result = asyncResult.GetValue();
                if (!result.IsSuccess()) {
                    return result;
                }

                // Processing the query result
                auto resultSet = result.GetResultSet(0);
                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    std::cout << "Series"
                        << ", Id: " << parser.ColumnParser("series_id").GetOptionalUint64().value()
                        << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8().value()
                        << std::endl;
                }

                return result;
            });
        });

        // Waiting for completion
        auto status = future.GetValueSync();
        if (!status.IsSuccess()) {
            std::cerr << "Query failed: " << status.GetIssues().ToString() << std::endl;
        }
    }
    ```

    {% endcut %}

    {% cut "Execution retry attempts when working with streaming queries" %}

    For executing streaming queries with automatic retries, the `StreamExecuteQuery` method is used.
    Streaming queries allow processing large volumes of data by receiving results in parts.

    Example code using `RetryQuerySync` with `StreamExecuteQuery`:


    ```c++
    #include <ydb-cpp-sdk/client/query/client.h>

    void StreamQueryWithRetry(NYdb::NQuery::TQueryClient client) {
        auto result = client.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
            auto query = R"(
                SELECT series_id, title, release_date
                FROM series
                WHERE series_id > 0;
            )";

            auto resultStreamQuery = session.StreamExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx()
            ).GetValueSync();

            if (!resultStreamQuery.IsSuccess()) {
                return resultStreamQuery;
            }

            // Processing results in parts
            bool eos = false;
            while (!eos) {
                auto streamPart = resultStreamQuery.ReadNext().ExtractValueSync();

                if (!streamPart.IsSuccess()) {
                    eos = true;
                    if (!streamPart.EOS()) {
                        return streamPart;
                    }
                    continue;
                }

                if (streamPart.HasResultSet()) {
                    auto rs = streamPart.ExtractResultSet();
                    NYdb::TResultSetParser parser(rs);
                    while (parser.TryNextRow()) {
                        std::cout << "Series"
                            << ", Id: " << parser.ColumnParser("series_id").GetOptionalUint64().value()
                            << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8().value()
                            << std::endl;
                    }
                }
            }

            return resultStreamQuery;
        });

        if (!result.IsSuccess()) {
            std::cerr << "Stream query failed: " << result.GetIssues().ToString() << std::endl;
        }
    }
    ```

    {% endcut %}

    {% cut "Configuration parameters retry attempts" %}

    The user can configure the behavior of the retry mechanism using the `TRetryOperationSettings` class:

    * `MaxRetries(uint32_t)` - maximum number of retries (default 10)
    * `Idempotent(bool)` - idempotency flag of the operation. Idempotent operations are retried for a wider list of errors
    * `RetryNotFound(bool)` - whether to retry operations that returned status `NOT_FOUND` (default true)
    * `MaxTimeout(TDuration)` - maximum execution time of all attempts
    * `FastBackoffSettings(TBackoffSettings)` - fast retry settings
    * `SlowBackoffSettings(TBackoffSettings)` - slow retry settings

    Example of using retry settings:


    ```c++
    #include <ydb-cpp-sdk/client/query/client.h>
    #include <ydb-cpp-sdk/client/retry/retry.h>

    void ExecuteWithCustomRetry(NYdb::NQuery::TQueryClient client) {
        auto retrySettings = NYdb::NRetry::TRetryOperationSettings()
            .Idempotent(true)
            .MaxRetries(20)
            .MaxTimeout(TDuration::Seconds(30));

        auto result = client.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
            auto query = R"(
                UPSERT INTO series (series_id, title)
                VALUES (10, "New Series");
            )";

            auto result = session.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()
            ).GetValueSync();

            if (!result.IsSuccess()) {
                return result;
            }

            // Processing the query result
            std::cout << "Query executed successfully" << std::endl;
            return result;
        }, retrySettings);

        if (!result.IsSuccess()) {
            std::cerr << "Operation failed: " << result.GetIssues().ToString() << std::endl;
        }
    }
    ```

    {% endcut %}

  - userver

    In `ydb::TableClient`, retry with correct error handling is implemented in all methods. userver automatically analyzes errors and performs retries according to their type.

    The user can configure the behavior of the retry mechanism using `ydb::OperationSettings` and `ydb::RetryTxSettings`:

    * `retries` - maximum number of retries
    * `is_idempotent` - idempotency flag of the operation. Idempotent operations are retried for a wider list of errors
    * `client_timeout_ms` or `timeout_ms` respectively - maximum execution time of all attempts
    * `get_session_timeout` (relevant only for `ydb::OperationSettings`) - timeout for getting a session
    * `get_session_settings`, `commit_settings`, and `rollback_settings` (relevant only for `ydb::RetryTxSettings`) - settings for session acquisition, commit, or transaction rollback queries

    `ydb::RetryTxSettings` is used only for the `ydb::TableClient::RetryTx` method, which executes an interactive transaction with retries on errors for the entire transaction.

    The [`ydb.operation-settings`](https://github.com/userver-framework/userver/blob/develop/ydb/src/ydb/component.yaml) section in the static config sets default values: if a field is not set in the code call (`std::nullopt` or zero where it means "not set"), the value from the config is used, otherwise from the code.

    {% cut "static config" %}

    ```yaml
    ydb:
        operation-settings:
            retries: 5
            client-timeout: 2s
            get-session-timeout: 10s
    ```

    {% endcut %}


    ```cpp
    #include <userver/ydb/table.hpp>

    void RetryExamples(ydb::TableClient& client) {
        client.ExecuteQuery(
            ydb::OperationSettings{
                .retries = 7,
                .is_idempotent = true,
            },
            ydb::Query{R"(
                UPSERT INTO series (series_id, title)
                VALUES (10, "New Series");
            )"}
        );

        client.RetryTx(
            ydb::RetryTxSettings{
                .retries = 3,
                .is_idempotent = true,
            },
            [](ydb::TxActor& tx) {
                tx.Execute(ydb::Query{R"(
                    UPSERT INTO series (series_id, title)
                    VALUES (11, "Other Series");
                )"});
            }
        );
    }
    ```

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

    In the {{ ydb-short-name }} Go SDK, proper error handling is implemented in several APIs:

    {% cut "Function retry general purpose" %}

    The main error handling logic is implemented by the helper function `retry.Retry`
    The details of executing retry queries are hidden as much as possible.
    The user can influence the logic of the `retry.Retry` function in two ways:

    * via context (you can set deadline and cancel)
    * via the operation idempotency flag `retry.WithIdempotent()`. By default, the operation is considered non-idempotent.

    The user passes their function to `retry.Retry`, which by its signature must return an error.
    If `nil` is returned from the user function, retry queries are stopped.
    If an error is returned from the user function, the {{ ydb-short-name }} Go SDK tries to identify this error and performs retries depending on it.

    Example code using the `retry.Retry` function:


    ```golang
    package main

    import (
        "context"
        "time"

        "github.com/ydb-platform/ydb-go-sdk/v3"
        "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
        db, err := ydb.Open(ctx,
            os.Getenv("YDB_CONNECTION_STRING"),
        )
        if err != nil {
            panic(err)
        }
        defer db.Close(ctx)
        var cancel context.CancelFunc
        // fix deadline for retries
        ctx, cancel := context.WithTimeout(ctx, time.Second)
        err = retry.Retry(
            ctx,
            func(ctx context.Context) error {
                whoAmI, err := db.Discovery().WhoAmI(ctx)
                if err != nil {
                    return err
                }
                fmt.Println(whoAmI)
                return nil
            },
            retry.WithIdempotent(true),
        )
        if err != nil {
            panic(err)
        }
    }
    ```

    {% endcut %}

    {% cut "Execution retry attempts when errors on object session {{ ydb-short-name }}" %}

    For retrying errors at the session level of the table service, {{ ydb-short-name }} provides the `db.Table().Do(ctx, op)` function, which returns a prepared session for executing queries. The `db.Table().Do(ctx, op)` function uses the `retry` package and also monitors the session lifetime {{ ydb-short-name }}. According to its signature, the user operation `op` must return an error or `nil` so that the driver can "understand" by the error type what to do: retry the operation or not, with or without a delay, on the same session or a new one. The user can influence the retry logic through the context and the idempotency flag, and {{ ydb-short-name }} Go SDK interprets the errors returned from `op`.

    Code example using the `db.Table().Do(ctx, op)` function:


    ```golang
    err := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
        desc, err = s.DescribeTableOptions(ctx)
        return
    }, table.WithIdempotent())
    if err != nil {
        return err
    }
    ```

    {% endcut %}

    {% cut "Execution retry attempts when errors on object interactive transaction {{ ydb-short-name }}" %}

    For retry error handling at the interactive transaction level of the {{ ydb-short-name }} table service, there is the `db.Table().DoTx(ctx, txOp)` function, which provides a prepared transaction {{ ydb-short-name }} on a session for executing queries.
    The `db.Table().DoTx(ctx, txOp)` function uses the `retry` package and also monitors the lifetime of {{ ydb-short-name }} sessions.
    From the user operation `txOp`, according to its signature, it is required to return an error or `nil` so that the driver can "understand" by the error type what to do: retry the operation or not, with or without a delay, on the same transaction or a new one.
    The user can influence the retry logic through the context and the idempotency flag, and the {{ ydb-short-name }} Go SDK interprets the errors returned from `op`.

    Example code using the `db.Table().DoTx(ctx, op)` function:


    ```golang
    err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
        _, err := tx.Execute(ctx,
            "DECLARE $id AS Int64; INSERT INTO test (id, val) VALUES($id, 'asd')",
            table.NewQueryParameters(table.ValueParam("$id", types.Int64Value(100500))),
        )
        return err
    }, table.WithIdempotent())
    if err != nil {
        return err
    }
    ```

    {% endcut %}

    {% cut "Queries to other services {{ ydb-short-name }}" %}

    (`db.Scripting()`, `db.Scheme()`, `db.Coordination()`, `db.Ratelimiter()`, `db.Discovery()`) also use the `retry.Retry` function internally to execute retry queries and do not require external helper functions for retries.

    {% endcut %}

  - database/sql

    The standard `database/sql` package uses internal retry logic based on the errors returned by the specific driver implementation.
    For example, in the [code](https://github.com/golang/go/tree/master/src/database/sql) of the `database/sql` package, a retry policy of three attempts can be found in many places:

    - 2 attempts to execute on an existing or new connection (if the `database/sql` connection pool is empty)
    - 1 attempt to execute on a new connection.

    In most cases, this retry policy is sufficient to survive temporary unavailability of {{ ydb-short-name }} nodes or {{ ydb-short-name }} session issues.

    {{ ydb-short-name }} Go SDK provides special functions for guaranteed execution of a user operation:

    {% cut "Execution retry attempts when errors on object connection `*sql.Conn`:" %}

    For retry error handling on the `*sql.Conn` connection object, there is a helper function `retry.Do(ctx, db, op)` that provides a prepared `*sql.Conn` connection for executing queries.
    The `retry.Do` function requires passing a context, a database object, and the user operation to be executed.
    From the client code, you can influence the retry logic through the context and the idempotency flag, while {{ ydb-short-name }} Go SDK interprets the errors returned from `op`.

    The user operation `op` must return an error or `nil`:

    - if the user function returns `nil`, retries are stopped;
    - if the user function returns an error, {{ ydb-short-name }} Go SDK tries to identify the error and, depending on it, makes retry attempts.

    Example code using the `retry.Do` function:


    ```golang
    import (
        "context"
        "database/sql"
        "fmt"
        "log"

        "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
        ...
        err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
            row = cc.QueryRowContext(ctx, `
                    PRAGMA TablePathPrefix("/local");
                    DECLARE $seriesID AS Uint64;
                    DECLARE $seasonID AS Uint64;
                    DECLARE $episodeID AS Uint64;
                    SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
                `,
                sql.Named("seriesID", uint64(1)),
                sql.Named("seasonID", uint64(1)),
                sql.Named("episodeID", uint64(1)),
            )
            var views sql.NullFloat64
            if err = row.Scan(&views); err != nil {
                return fmt.Errorf("cannot scan views: %w", err)
            }
            if views.Valid {
                return fmt.Errorf("unexpected valid views: %v", views.Float64)
            }
            log.Printf("views = %v", views)
            return row.Err()
        }, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
        if err != nil {
            log.Printf("retry.Do failed: %v\n", err)
        }
    }
    ```

    {% endcut %}

    {% cut "Execution retry attempts when errors on object interactive transaction `*sql.Tx`:" %}

    For retrying errors on the interactive transaction object `*sql.Tx`, there is a helper function `retry.DoTx(ctx, db, op)` that provides a prepared transaction `*sql.Tx` for executing queries.
    You need to pass a context, a database object, and a user operation to the function `retry.DoTx`.
    The function receives a prepared transaction `*sql.Tx`, on which you should execute queries to {{ ydb-short-name }}.
    From the client code, you can influence the retry logic through the context and the idempotency flag of the operation, and {{ ydb-short-name }} Go SDK in turn interprets the errors returned from `op`.

    The user operation `op` must return an error or `nil`:

    - if the user function returns `nil`, then retries are stopped;
    - if the user function returns an error, {{ ydb-short-name }} Go SDK tries to identify this error and, depending on it, makes retry attempts.

    The `retry.DoTx` function uses the read-write transaction isolation mode `sql.LevelDefault` by default, which can be changed via the `retry.WithTxOptions` option.

    Code example using the `retry.Do` function:


    ```golang
    import (
        "context"
        "database/sql"
        "fmt"
        "log"

        "github.com/ydb-platform/ydb-go-sdk/v3/retry"
    )

    func main() {
        ...
        err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
            row := tx.QueryRowContext(ctx,`
                    PRAGMA TablePathPrefix("/local");
                    DECLARE $seriesID AS Uint64;
                    DECLARE $seasonID AS Uint64;
                    DECLARE $episodeID AS Uint64;
                    SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
                `,
                sql.Named("seriesID", uint64(1)),
                sql.Named("seasonID", uint64(1)),
                sql.Named("episodeID", uint64(1)),
            )
            var views sql.NullFloat64
            if err = row.Scan(&views); err != nil {
                return fmt.Errorf("cannot select current views: %w", err)
            }
            if !views.Valid {
                return fmt.Errorf("unexpected invalid views: %v", views)
            }
            t.Logf("views = %v", views)
            if views.Float64 != 1 {
                return fmt.Errorf("unexpected views value: %v", views)
            }
            return nil
        }, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)), retry.WithTxOptions(&sql.TxOptions{
            Isolation: sql.LevelSnapshot,
            ReadOnly:  true,
        }))
        if err != nil {
            log.Printf("do tx failed: %v\n", err)
        }
    }
    ```

    {% endcut %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    In {{ ydb-short-name }} Java SDK, the retry mechanism is implemented as a helper class `SessionRetryContext`. This class is constructed using the `SessionRetryContext.create` method, which requires passing an implementation of the `SessionSupplier` interface — typically an instance of the `TableClient` or `QueryClient` class.

    Additionally, the user can set some other options:

    * `maxRetries(int maxRetries)` — maximum number of operation retries, does not include the first execution. Default value `10`
    * `retryNotFound(boolean retryNotFound)` — option to retry operations that returned status `NOT_FOUND`. Enabled by default.
    * `idempotent(boolean idempotent)` — idempotency flag for operations. Idempotent operations will be retried for a wider list of errors. Disabled by default.

    To run operations with retries, the `SessionRetryContext` class provides two methods:

    * `CompletableFuture<Status> supplyStatus` — executes an operation that returns a status. Takes a lambda `Function<Session, CompletableFuture<Status>> fn` as an argument
    * `CompletableFuture<Result<T>> supplyResult` — executes an operation that returns data. Takes a lambda `Function<Session, CompletableFuture<Result<T>>> fn` as an argument

    When using the `SessionRetryContext` class, note that the operation will be re-executed in the following cases:

    * The lambda returned a [retryable](../../reference/ydb-sdk/error_handling.md) error code
    * During lambda execution, `UnexpectedResultException` was called with a [retryable](../../reference/ydb-sdk/error_handling.md) error code

      {% cut "Example code, using SessionRetryContext.supplyStatus:" %}

      ```java
      private void createTable(TableClient tableClient, String database, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          TableDescription pets = TableDescription.newBuilder()
                  .addNullableColumn("species", PrimitiveType.Text)
                  .addNullableColumn("name", PrimitiveType.Text)
                  .addNullableColumn("color", PrimitiveType.Text)
                  .addNullableColumn("price", PrimitiveType.Float)
                  .setPrimaryKeys("species", "name")
                  .build();

          String tablePath = database + "/" + tableName;
          retryCtx.supplyStatus(session -> session.createTable(tablePath, pets))
                  .join().expectSuccess();
      }
      ```

      {% endcut %}

      {% cut "Example code, using SessionRetryContext.supplyResult:" %}

      ```java
      private void selectData(TableClient tableClient, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          String selectQuery
                  = "DECLARE $species AS Text;"
                  + "DECLARE $name AS Text;"
                  + "SELECT * FROM " + tableName + " "
                  + "WHERE species = $species AND name = $name;";

          Params params = Params.of(
                  "$species", PrimitiveValue.newText("cat"),
                  "$name", PrimitiveValue.newText("Tom")
          );

          DataQueryResult data = retryCtx
                  .supplyResult(session -> session.executeDataQuery(selectQuery, TxControl.onlineRo(), params))
                  .join().getValue();

          ResultSetReader rsReader = data.getResultSet(0);
          logger.info("Result of select query:");
          while (rsReader.next()) {
              logger.info("  species: {}, name: {}, color: {}, price: {}",
                      rsReader.getColumn("species").getText(),
                      rsReader.getColumn("name").getText(),
                      rsReader.getColumn("color").getText(),
                      rsReader.getColumn("price").getFloat()
              );
          }
      }
      ```

      {% endcut %}

  - JDBC

    Retries at the `SessionRetryContext` level belong to the native API (`TableClient` / `QueryClient`). When working via JDBC, use retries at the application level or connect the native transport and client as described in the [Driver initialization](./init.md) section.

  {% endlist %}

- Python

  {% list tabs %}

  - Native SDK

    In {{ ydb-short-name }} Python SDK, retry execution is implemented in `QuerySessionPool` using the `RetrySettings` class to configure retry parameters. The `RetrySettings` class supports the following options:

    * `max_retries` — maximum number of retry attempts (default 10)
    * `idempotent` — idempotency flag for the operation. Idempotent operations are retried for a wider list of errors (default False)
    * `backoff_ceiling`, `backoff_slot_duration` — parameters of the exponential backoff algorithm
    * `fast_backoff_settings`, `slow_backoff_settings` — settings for fast and slow retries

    To execute queries with retries, `QuerySessionPool` provides the `retry_operation_sync` and `execute_with_retries` methods. The `execute_with_retries` method is intended for single queries with implicit transaction mode. For other cases (explicit transactions, multiple operations in one transaction), use `retry_operation_sync`.

    Code example using execute_with_retries:


    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        result_sets = pool.execute_with_retries(
            "SELECT series_id, title FROM series WHERE series_id = 1;",
            retry_settings=ydb.RetrySettings(idempotent=True),
        )
        # ...
    ```


    Code example using retry_operation_sync:


    ```python
    import ydb

    def execute_query(pool: ydb.QuerySessionPool):
        def callee(session: ydb.QuerySession):
              with session.transaction().execute(
                  "SELECT 1",
                  commit_tx=True,
              ) as result_sets:
                  pass

        result = pool.retry_operation_sync(
            callee,
            retry_settings=ydb.RetrySettings(max_retries=20, idempotent=True),
        )
        # ...
    ```

  - Native SDK (Asyncio)

    Code example using execute_with_retries:


    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        result_sets = await pool.execute_with_retries(
            "SELECT series_id, title FROM series WHERE series_id = 1;",
            retry_settings=ydb.RetrySettings(idempotent=True),
        )
        # ...
    ```


    Code example using retry_operation_sync:


    ```python
    import ydb

    async def execute_query(pool: ydb.aio.QuerySessionPool):
        async def callee(session):
            async with session.transaction(tx_mode=ydb.QuerySerializableReadWrite()) as tx:
                async with await tx.execute("SELECT 1", commit_tx=True) as result_sets:
                    pass

        await pool.retry_operation_async(
            callee,
            retry_settings=ydb.RetrySettings(max_retries=20, idempotent=True),
        )
        # ...
    ```

  - SQLAlchemy

    When using {{ ydb-short-name }} via SQLAlchemy, retries happen under the hood and cannot be configured externally.

  {% endlist %}

- C#

  In {{ ydb-short-name }} C# SDK, retries are implemented at two levels.

  {% list tabs %}

  - OpenRetryableConnectionAsync

    The `OpenRetryableConnectionAsync` method creates a connection with automatic retries on transient errors. A connection obtained this way does not support interactive transactions — use `ExecuteInTransactionAsync` for working with transactions.


    ```C#
    using Ydb.Sdk.Ado;

    await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");

    await using var connection = await dataSource.OpenRetryableConnectionAsync();
    var command = new YdbCommand("SELECT series_id, title FROM series WHERE series_id = $series_id", connection);
    command.Parameters.Add(new YdbParameter("$series_id", YdbDbType.Uint64, 1U));

    await using var reader = await command.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        Console.WriteLine($"series_id: {reader.GetUint64(0)}, title: {reader.GetString(1)}");
    }
    ```

  - ExecuteInTransactionAsync

    The `ExecuteInTransactionAsync` method executes multiple operations within a single transaction with automatic retry on conflicts:


    ```C#
    using Ydb.Sdk.Ado;

    await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");

    await dataSource.ExecuteInTransactionAsync(async connection =>
    {
        var command = connection.CreateCommand();
        command.CommandText = "UPSERT INTO series (series_id, title) VALUES (1, \"IT Crowd\")";
        await command.ExecuteNonQueryAsync();
    });
    ```

  {% endlist %}

- JavaScript

  Retries and reconnections are handled inside the SDK; no separate configuration is needed by the user.

  The retrier itself is available in a separate package `@ydbjs/retry`.


  ```javascript
  import { retry } from '@ydbjs/retry'

  let attempts = 0
  const result = retry({ retry: isError, budget: 3 }, async () => {
    if (attempts >= 2) {
      return 'success'
    }

    attempts++
    throw new Error('test error')
  })
  ```

- Rust

  Retries for queries via Query Service are handled by `QueryClient`: helper methods for executing a single transactional SQL query (`query_row`, `exec`, etc.) are retried automatically; for multiple operations in one transaction — `retry_transaction`.


  ```rust
  use ydb::{AccessTokenCredentials, ClientBuilder, YdbResult};

  #[tokio::main]
  async fn main() -> YdbResult<()> {
      let client = ClientBuilder::new_from_connection_string(
          "grpc://localhost:2136?database=local",
      )?
      .with_credentials(AccessTokenCredentials::from("..."))
      .client()?;

      client.wait().await?;

      let mut qc = client.query_client().clone_with_idempotent_operations(true);

      // One SQL query on the query client: internal retries
      let mut row = qc
          .query_row("SELECT series_id, title FROM series WHERE series_id = 1")
          .await?;

      // Multiple operations in a single transaction with retries
      let title: String = qc
          .retry_transaction(async |tx| {
              let mut row = tx
                  .query_row("SELECT series_id, title FROM series WHERE series_id = 1")
                  .await?;
              Ok(row.remove_field_by_name("title")?.try_into()?)
          })
          .await?;

      Ok(())
  }
  ```

- PHP

  In the {{ ydb-short-name }} PHP SDK, retries for Table API queries are set via `Table::retryTransaction()` (transaction + commit + retries on supported errors) or `Table::retrySession()` (a single session without the 'whole transaction' wrapper). The second argument `retryTransaction` is an idempotency flag (`true` expands the set of errors on which a retry is performed).

  Example with `retryTransaction`:


  ```php
  <?php

  use YdbPlatform\Ydb\Session;
  use YdbPlatform\Ydb\Ydb;

  $ydb = new Ydb($config);

  $result = $ydb->table()->retryTransaction(
      function (Session $session) {
          return $session->query(
              'SELECT series_id, title FROM series WHERE series_id = 1;'
          );
      },
      true
  );

  // $result->rows(), $result->rowCount(), ...
  ```

{% endlist %}
