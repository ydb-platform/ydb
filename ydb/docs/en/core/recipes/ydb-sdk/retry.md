# Retrying

{% include [work in progress message](_includes/addition.md) %}

{{ ydb-short-name }} is a distributed database management system with automatic load scaling.
Routine maintenance can be carried out on the server side, with server racks or entire data centers temporarily shut down.
This may result in errors arising from {{ ydb-short-name }} operation.
There are different response scenarios depending on the error type.
{{ ydb-short-name }}
 To ensure high database availability, SDKs provide built-in tools for retries,
accounting for error types and responses to them.

Below are code examples showing the {{ ydb-short-name }} SDK built-in tools for retries:

{% list tabs %}

- Go (native)

   In the {{ ydb-short-name }} Go SDK, correct error handling is implemented by several programming interfaces:

   {% cut "General-purpose repeat function" %}

   The basic logic of error handling is implemented by the helper `retry.Retry` function
   The details of repeat query execution are mostly hidden.
   The user can affect the logic of the `retry.Retry` function in two ways:
   * Via the context (where you can set the deadline and cancel)
   * Via the operation's idempotency flag `retry.WithIdempotent()`. By default, the operation is considered non-idempotent.

   The user passes a custom function to `retry.Retry` that returns an error by its signature.
   If the custom function returns `nil`, then repeat queries stop.
   If the custom function returns an error, the {{ ydb-short-name }} Go SDK tries to identify this error and executes retries depending on it.

   Example of the code that uses the `retry.Retry` function:
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
           },
           retry.WithIdempotent(true),
       )
       if err != nil {
           panic(err)
       }
   }
   ```

   {% endcut %}

   {% cut "Repeat attempts in case of failed {{ ydb-short-name }} session objects" %}

   For repeat error handling at the level of a {{ ydb-short-name }} table service session, you can use the `db.Table().Do(ctx, op)` function, which provides a prepared session for query execution.
   `db.Table().Do(ctx, op)` uses the `retry` package and tracks the lifetime of the {{ ydb-short-name }} sessions.
   Based on its signature, the user's operation `op` should return an error or `nil` so that the driver can "decide" what to do based on the error type: repeat the operation or not, with delay or without, and in this session or a new one.
   The user can affect the logic of repeat queries using the context and the idempotence flag, while the {{ ydb-short-name }} Go SDK interprets errors returned by `op`.

   Example of the code that uses the `db.Table().Do(ctx, op)` function:
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

   {% cut "Repeat attempts in case of failed {{ ydb-short-name }} interactive transaction objects" %}

   For repeat error handling at the level of a {{ ydb-short-name }} table service interactive transaction, you can use the `db.Table().DoTx(ctx, txOp)` function, which provides a {{ ydb-short-name }} prepared session transaction for query execution.
   `db.Table().DoTx(ctx, txOp)` uses the `retry` package and tracks the lifetime of the {{ ydb-short-name }} sessions.
   Based on its signature, the user's operation `txOp` should return an error or `nil` so that the driver can "decide" what to do based on the error type: repeat the operation or not, with delay or without, and in this transaction or a new one.
   The user can affect the logic of repeat queries using the context and the idempotence flag, while the {{ ydb-short-name }} Go SDK interprets errors returned by `op`.

   Example of the code that uses the `db.Table().DoTx(ctx, op)` function:
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

   {% cut "Queries to other {{ ydb-short-name }} services" %}

   (`db.Scripting()`, `db.Scheme()`, `db.Coordination()`, `db.Ratelimiter()`, `db.Discovery()`) also use the `retry.Retry` function inside to execute repeat queries and don't require external auxiliary functions for repeats.

   {% endcut %}

- Go (database/sql)

   The standard `database/sql` package uses the internal logic of repeats based on the errors a specific driver implementation returns.
   For example, the `database/sql` [code](https://github.com/golang/go/tree/master/src/database/sql) frequently shows the three-attempt repeats policy:
   - Two attempts at a present connection or new one (if the `database/sql` connection pool is empty).
   - One attempt at a new connection.

   This repeat policy is mostly enough to survive temporary unavailability of {{ ydb-short-name }} nodes or issues with a {{ ydb-short-name }} session.

   The {{ ydb-short-name }} Go SDK provides special functions to ensure execution of a user's operation:

   {% cut "Repeat attempts in case of failed `*sql.Conn` connection objects:" %}

   For repeat error handling at `*sql.Conn` connection objects, you can use the auxiliary `retry.Do(ctx, db, op)` function, which provides a prepared `*sql.Conn` session for query execution.
   You need to pass the context, database object, and the user's operation for execution to the `retry.Do` function.
   The user's code can affect the logic of repeat queries using the context and the idempotence flag, while the {{ ydb-short-name }} Go SDK, in turn, interprets errors returned by `op`.

   The user's `op` operation must return an error or `nil`:
   - If the custom function returns `nil`, then repeat queries stop.
   - If the custom function returns an error, the {{ ydb-short-name }} Go SDK tries to identify this error and performs retries depending on it.

   Example of the code that uses the `retry.Do` function:
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

   {% cut "Repeat attempts in case of failed `*sql.Tx` interactive transaction objects:" %}

   For repeat error handling at `*sql.Tx` interactive transaction objects, you can use the auxiliary `retry.DoTx(ctx, db, op)` function, which provides a prepared `*sql.Tx` transaction for query execution.
   You need to pass the context, database object, and the user's operation for execution to the `retry.DoTx` function.
   The function is passed a prepared `*sql.Tx` transaction, where queries to {{ ydb-short-name }} should be executed.
   The user's code can affect the logic of repeat queries using the context and the operation idempotence flag, while the {{ ydb-short-name }} Go SDK, in turn, interprets errors returned by `op`.

   The user's `op` operation must return an error or `nil`:
   - If the custom function returns `nil`, then repeat queries stop.
   - If the custom function returns an error, the {{ ydb-short-name }} Go SDK tries to identify this error and performs retries depending on it.

   By default, `retry.DoTx` uses the read-write isolation mode of the `sql.LevelDefault` transaction and you can change it using the `retry.WithTxOptions` parameter.

   Example of the code that uses the `retry.Do` function:
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

- Java

   In the {{ ydb-short-name }} Java SDK, repeat queries are implemented by the `SessionRetryContext` helper class. This class is constructed with the `SessionRetryContext.create` method to which you pass the `SessionSupplier` interface implementation (usually an instance of the `TableClient` class or the `QueryClient` class).
   Additionally, the user can specify some other options
   * `maxRetries(int maxRetries)`: The maximum number of operation retries, not counting the first execution. Default value: `10`
   * `retryNotFound(boolean retryNotFound)`: The option to retry operations that returned the `NOT_FOUND` status. Enabled by default.
   * `idempotent(boolean idempotent)`: Indicates idempotence of operations. Idempotent operations will be retried for a broader range of errors. Disabled by default.

   The `SessionRetryContext` class provides two methods to run operations with retries.
   * `CompletableFuture<Status> supplyStatus`: Executing the operation that returns the status. As an argument, it accepts the lambda `Function<Session, CompletableFuture<Status>> fn`
   * `CompletableFuture<Result<T>> supplyResult`: Executing the operation that returns data. As an argument, it accepts the lambda `Function<Session, CompletableFuture<Result<T>>> fn`

   When using the `SessionRetryContext` class, make sure that the operation will be retried in the following cases
   * The lambda function returned a [retryable](../../reference/ydb-sdk/error_handling.md) error code
   * The lambda function invoked an `UnexpectedResultException` with a [retryable](../../reference/ydb-sdk/error_handling.md) error code



      {% cut "Sample code using SessionRetryContext.supplyStatus:" %}

      ```java
      private void createTable(TableClient tableClient, String database, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          TableDescription pets = TableDescription.newBuilder()
                  .addNullableColumn("species", PrimitiveType.utf8())
                  .addNullableColumn("name", PrimitiveType.utf8())
                  .addNullableColumn("color", PrimitiveType.utf8())
                  .addNullableColumn("price", PrimitiveType.float32())
                  .setPrimaryKeys("species", "name")
                  .build();

          String tablePath = database + "/" + tableName;
          retryCtx.supplyStatus(session -> session.createTable(tablePath, pets))
                  .join().expect("ok");
      }
      ```

      {% endcut %}

      {% cut "Sample code using SessionRetryContext.supplyResult:" %}

      ```java
      private void selectData(TableClient tableClient, String tableName) {
          SessionRetryContext retryCtx = SessionRetryContext.create(tableClient).build();
          String selectQuery
                  = "DECLARE $species AS Utf8;"
                  + "DECLARE $name AS Utf8;"
                  + "SELECT * FROM " + tableName + " "
                  + "WHERE species = $species AND name = $name;";

          Params params = Params.of(
                  "$species", PrimitiveValue.utf8("cat"),
                  "$name", PrimitiveValue.utf8("Tom")
          );

          DataQueryResult data = retryCtx
                  .supplyResult(session -> session.executeDataQuery(selectQuery, TxControl.onlineRo(), params))
                  .join().expect("ok");

          ResultSetReader rsReader = data.getResultSet(0);
          logger.info("Result of select query:");
          while (rsReader.next()) {
              logger.info("  species: {}, name: {}, color: {}, price: {}",
                      rsReader.getColumn("species").getUtf8(),
                      rsReader.getColumn("name").getUtf8(),
                      rsReader.getColumn("color").getUtf8(),
                      rsReader.getColumn("price").getFloat32()
              );
          }
      }
      ```

      {% endcut %}

{% endlist %}
