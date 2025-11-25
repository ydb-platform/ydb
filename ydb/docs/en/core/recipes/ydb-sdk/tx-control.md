# Setting up the transaction execution mode

To run your queries, first you need to specify the [transaction execution mode](../../concepts/transactions.md#modes) in the {{ ydb-short-name }} SDK.

Below are code examples showing the {{ ydb-short-name }} SDK built-in tools to create an object for the *transaction execution mode*.

## Serializable {#serializable}

{% list tabs group=lang %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/query"
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
     row, err := db.Query().QueryRow(ctx, "SELECT 1",
       query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
     )
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
     // work with row
     _ = row
   }
   ```

- Go (database/sql)

   ```go
   package main

   import (
     "context"
     "database/sql"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/retry"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
     )
     if err != nil {
       panic(err)
     }
     defer nativeDriver.Close(ctx)

     connector, err := ydb.Connector(nativeDriver)
     if err != nil {
       panic(err)
     }
     defer connector.Close()

     db := sql.OpenDB(connector)
     defer db.Close()

     // Serializable Read-Write mode is used by default for transactions
     err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
       row := tx.QueryRowContext(ctx, "SELECT 1")
       var result int
       return row.Scan(&result)
     }, retry.WithIdempotent(true))
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
   }
   ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;

  $config = [
      // Database path
      'database'    => '/ru-central1/b1glxxxxxxxxxxxxxxxx/etn0xxxxxxxxxxxxxxxx',

      // Database endpoint
      'endpoint'    => 'ydb.serverless.yandexcloud.net:2135',

      // Auto discovery (dedicated server only)
      'discovery'   => false,

      // IAM config
      'iam_config'  => [
          // 'root_cert_file' => './CA.pem',  Root CA file (uncomment for dedicated server only)
      ],

      'credentials' => new AccessTokenAuthentication('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') // use from reference/ydb-sdk/auth
  ];

  $ydb = new Ydb($config);
  $ydb->table()->retryTransaction(function(Session $session){
    $session->query('SELECT 1;');
  })
  ```

{% endlist %}

## Online Read-Only {#online-read-only}

{% list tabs group=lang %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/query"
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
     row, err := db.Query().QueryRow(ctx, "SELECT 1",
       query.WithTxControl(
         query.OnlineReadOnlyTxControl(query.WithInconsistentReads()),
       ),
     )
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
     // work with row
     _ = row
   }
   ```

- Go (database/sql)

   ```go
   package main

   import (
     "context"
     "database/sql"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/retry"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
     )
     if err != nil {
       panic(err)
     }
     defer nativeDriver.Close(ctx)

     connector, err := ydb.Connector(nativeDriver)
     if err != nil {
       panic(err)
     }
     defer connector.Close()

     db := sql.OpenDB(connector)
     defer db.Close()

     // Online Read-Only - a read mode providing access to the latest data
     // without strict consistency guarantees
     err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
       row := tx.QueryRowContext(ctx, "SELECT 1")
       var result int
       return row.Scan(&result)
     }, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
       Isolation: sql.LevelReadCommitted,
       ReadOnly:  true,
     }))
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
   }
   ```

{% endlist %}

## Stale Read-Only {#stale-read-only}

{% list tabs group=lang %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/query"
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
     row, err := db.Query().QueryRow(ctx, "SELECT 1",
       query.WithTxControl(query.StaleReadOnlyTxControl()),
     )
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
     // work with row
     _ = row
   }
   ```

- Go (database/sql)

   The Stale Read-Only mode is not directly supported in the standard `database/sql` interface. It is recommended to use the native Go SDK for this transaction mode.

{% endlist %}

## Snapshot Read-Only {#snapshot-read-only}

{% list tabs group=lang %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/query"
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
     row, err := db.Query().QueryRow(ctx, "SELECT 1",
       query.WithTxControl(query.SnapshotReadOnlyTxControl()),
     )
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
     // work with row
     _ = row
   }
   ```

- Go (database/sql)

   ```go
   package main

   import (
     "context"
     "database/sql"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/retry"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
     )
     if err != nil {
       panic(err)
     }
     defer nativeDriver.Close(ctx)

     connector, err := ydb.Connector(nativeDriver)
     if err != nil {
       panic(err)
     }
     defer connector.Close()

     db := sql.OpenDB(connector)
     defer db.Close()

     // Snapshot Read-Only - provides consistent reading of data at a specific point in time
     err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
       row := tx.QueryRowContext(ctx, "SELECT 1")
       var result int
       return row.Scan(&result)
     }, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
       Isolation: sql.LevelSnapshot,
       ReadOnly:  true,
     }))
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
   }
   ```

{% endlist %}

## Snapshot Read-Write {#snapshot-read-write}

{% list tabs group=lang %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/query"
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
     row, err := db.Query().QueryRow(ctx, "SELECT 1",
       query.WithTxControl(query.SnapshotReadWriteTxControl(query.CommitTx())),
     )
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
     // work with row
     _ = row
   }
   ```

- Go (database/sql)

   ```go
   package main

   import (
     "context"
     "database/sql"
     "fmt"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/retry"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
     )
     if err != nil {
       panic(err)
     }
     defer nativeDriver.Close(ctx)

     connector, err := ydb.Connector(nativeDriver)
     if err != nil {
       panic(err)
     }
     defer connector.Close()

     db := sql.OpenDB(connector)
     defer db.Close()

     // Snapshot Read-Write - provides consistent reading of data at a specific point in time
     // with write capability
     err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
       row := tx.QueryRowContext(ctx, "SELECT 1")
       var result int
       return row.Scan(&result)
     }, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
       Isolation: sql.LevelSnapshot,
       ReadOnly:  false,
     }))
     if err != nil {
       fmt.Printf("unexpected error: %v", err)
     }
   }
   ```

{% endlist %}
