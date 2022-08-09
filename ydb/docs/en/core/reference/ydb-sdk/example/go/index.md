# App in Go

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic) that uses the {{ ydb-short-name }} [Go SDK](https://github.com/ydb-platform/ydb-go-sdk/v3).

## Downloading and starting {#download}

The startup script given below uses [git](https://git-scm.com/downloads) and [Go](https://go.dev/doc/install). Be sure to install the [YDB Go SDK](../../install.md) first.

Create a working directory and use it to run from the command line the command to clone the GitHub repository:

```bash
git clone https://github.com/ydb-platform/ydb-go-examples/
```

Next, from the same working directory, run the command to start the test app. The command will differ depending on the database to connect to.

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

To work with `YDB` in `Go`, import the `ydb-go-sdk` driver package:

```go
import (
  // general imports
  "context"
  "path"

  // imports of ydb-go-sdk packages
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/table" // to work with the table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/options" // to work with the table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result" // to work with the table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named" // to work with the table service
  "github.com/ydb-platform/ydb-go-sdk-auth-environ" // for authentication using environment variables
  "github.com/ydb-platform/ydb-go-yc" // to work with YDB in Yandex.Cloud
)
```

App code snippet for driver initialization:

```go
ctx := context.Background()
// connection string
dsn := "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu"
// IAM token
token := "t1.9euelZrOy8aVmZKJm5HGjceMkMeVj-..."
// creating a DB connection object, which is the input point for YDB services
db, err := ydb.Open(ctx,
  dsn,
//  yc.WithInternalCA(), // using Yandex.Cloud certificates
  ydb.WithAccessTokenCredentials(token), // token-based authentication
//  ydb.WithAnonimousCredentials(), // anonymous authentication (for example, in docker ydb)
//  yc.WithMetadataCredentials(token), // authentication from inside a VM in Yandex.Cloud or a function in Yandex Functions
//  yc.WithServiceAccountKeyFileCredentials("~/.ydb/sa.json"), // authentication in Yandex.Cloud using a service account file
//  environ.WithEnvironCredentials(ctx), // authentication using environment variables
)
if err != nil {
  // connection error handling
}
// closing the driver at the end of the program is mandatory
defer func() {
  _ = db.Close(ctx)
}
```

The `db` object is an input point for working with `YDB` services.
To work with the table service, use the `db.Table()` client.
The client of the table service provides an `API` for making queries to tables.
The most popular method is `db.Table().Do(ctx, op)`. It implements background session creation and repeated attempts to perform the `op` user operation where the created session is passed to the user-defined code.
The session has an exhaustive `API` that lets you perform `DDL`, `DML`, `DQL`, and `TCL` requests.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

To create tables, use the `table.Session.CreateTable()` method:

```go
err = db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    return s.CreateTable(ctx, path.Join(db.Name(), "series"),
      options.WithColumn("series_id", types.Optional(types.TypeUint64)),
      options.WithColumn("title", types.Optional(types.TypeUTF8)),
      options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
      options.WithColumn("release_date", types.Optional(types.TypeDate)),
      options.WithColumn("comment", types.Optional(types.TypeUTF8)),
      options.WithPrimaryKeyColumn("series_id"),
    )
  },
)
if err != nil {
  // handling the situation when the request failed
}
```

You can use the `table.Session.DescribeTable()` method to output information about the table structure and make sure that it was properly created:

```go
err = db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    desc, err := s.DescribeTable(ctx, path.Join(db.Name(), "series"))
    if err != nil {
      return
    }
    log.Printf("> describe table: %s\n", tableName)
    for _, c := range desc.Columns {
      log.Printf("  > column, name: %s, %s\n", c.Type, c.Name)
    }
    return
  }
)
if err != nil {
  // handling the situation when the request failed
}
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

To execute YQL queries, use the `table.Session.Execute()` method.
The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `table.TxControl` structure.

```go
var (
  readTx = table.TxControl(
    table.BeginTx(
      table.WithOnlineReadOnly(),
    ),
    table.CommitTx(),
  )
)
err := db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    var (
      res   result.Result
      id    *uint64 // pointer - for optional results
      title *string // pointer - for optional results
      date  *time.Time // pointer - for optional results
    )
    _, res, err = s.Execute(
      ctx,
      readTx,
      `
        DECLARE $seriesID AS Uint64;
        SELECT
          series_id,
          title,
          release_date
        FROM
          series
        WHERE
          series_id = $seriesID;
      `,
      table.NewQueryParameters(
        table.ValueParam("$seriesID", types.Uint64Value(1)), // substitution in the query condition
      ),
    )
    if err != nil {
      return err
    }
    defer func() {
      _ = res.Close() // making sure the result is closed
    }()
    log.Printf("> select_simple_transaction:\n")
    for res.NextResultSet(ctx) {
      for res.NextRow() {
        // passing column names from the scanning line to ScanNamed,
        // addresses (and data types) to assign query results to
        err = res.ScanNamed(
          named.Optional("series_id", &id),
          named.Optional("title", &title),
          named.Optional("release_date", &date),
        )
        if err != nil {
          return err
        }
        log.Printf(
          "  > %d %s %s\n",
          *id, *title, *date,
        )
      }
    }
    return res.Err()
  },
)
if err != nil {
  // handling the query execution error
}
```

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

To execute scan queries, use the `table.Session.StreamExecuteScanQuery()` method.

```go
var (
  query = `
    DECLARE $series AS List<UInt64>;
    SELECT series_id, season_id, title, first_aired
    FROM seasons
    WHERE series_id IN $series
  `
  res result.StreamResult
)
err = c.Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    res, err = s.StreamExecuteScanQuery(ctx, query,
      table.NewQueryParameters(
        table.ValueParam("$series",
          types.ListValue(
            types.Uint64Value(1),
            types.Uint64Value(10),
          ),
        ),
      ),
    )
    if err != nil {
      return err
    }
    defer func() {
      _ = res.Close() // making sure the result is closed
    }()
    var (
      seriesID uint64
      seasonID uint64
      title    string
      date     time.Time
    )
    log.Print("\n> scan_query_select:")
    for res.NextResultSet(ctx) {
      if err = res.Err(); err != nil {
        return err
      }
      for res.NextRow() {
        // named.OptionalOrDefault lets you "deploy" optional
        // results or use the default value of the go type
        err = res.ScanNamed(
          named.OptionalOrDefault("series_id", &seriesID),
          named.OptionalOrDefault("season_id", &seasonID),
          named.OptionalOrDefault("title", &title),
          named.OptionalOrDefault("first_aired", &date),
        )
        if err != nil {
          return err
        }
        log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesID, seasonID, title, date)
      }
    }
    return res.Err()
  },
)
if err != nil {
  // handling the query execution error
}
```

{% note info %}

Sample code of a test app that uses archived of versions the Go SDK:

- [github.com/yandex-cloud/ydb-go-sdk](https://github.com/yandex-cloud/ydb-go-sdk/tree/v1.5.1) is available at this [link](../archive/example-go-v1.md),
- [github.com/yandex-cloud/ydb-go-sdk/v2](https://github.com/yandex-cloud/ydb-go-sdk/tree/v2.11.2) is available at this [link](../archive/example-go-v2.md).

{% endnote %}

