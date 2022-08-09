In the {{ ydb-short-name }} Go SDK, correct error handling is implemented by several programming interfaces:

* The basic logic of error handling is implemented by the helper `retry.Retry` function.
  The details of making request retries are hidden as much as possible.
  The user can affect the logic of executing the `retry.Retry` function in two ways:
   * Via the context (where you can set the deadline and cancel).
   * Via the operation's idempotency flag `retry.WithIdempotent()`. By default, the operation is considered non-idempotent.

  The user passes a custom function to `retry.Retry` that returns an error by its signature.
If the custom function returns `nil`, then repeat queries stop.
If the custom function returns an error, the {{ ydb-short-name }} Go SDK tries to identify this error and executes retries depending on it.

  {% cut "Example of code, using `retry.Retry` function:" %}

    ```go
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
        defer func() {
            _ = db.Close(ctx)
        }()
        var cancel context.CancelFunc
        // fix deadline for retries
        ctx, cancel := context.WithTimeout(ctx, time.Second)
        err = retry.Retry(ctx,
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

* The `db.Table()` table query service immediately provides the `table.Client` programming interface that uses the `retry` package and tracks the lifetime of the {{ ydb-short-name }} sessions.
  Two public functions are available to the user: `db.Table().Do(ctx, op)` (where `op` provides a session) and `db.Table().DoTx(ctx, op)` (where `op` provides a transaction).
  As in the previous case, the user can affect the logic of repeat queries using the context and the idempotence flag, while the {{ ydb-short-name }} Go SDK interprets errors returned by `op`.

* Queries to other {{ ydb-short-name }} services (`db.Scripting()`, `db.Scheme()`, `db.Coordination()`, `db.Ratelimiter()`, and `db.Discovery()`) also use the `retry.Retry` function internally to make repeat queries.

