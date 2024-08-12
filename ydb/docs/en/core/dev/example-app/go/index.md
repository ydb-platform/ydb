# App in Go

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic) that uses the {{ ydb-short-name }} [Go SDK](https://github.com/ydb-platform/ydb-go-sdk/v3).

## Downloading and starting {#download}

The following execution scenario is based on [git](https://git-scm.com/downloads) and [Go](https://go.dev/doc/install). Be sure to install the [YDB Go SDK](../../../reference/ydb-sdk/install.md).

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
  // general imports from standard library
  "context"
  "log"
  "path"

  // importing the packages ydb-go-sdk
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/table" // needed to work with table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/options" // needed to work with table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result" // needed to work with table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named" // needed to work with table service
  "github.com/ydb-platform/ydb-go-sdk/v3/table/types" // needed to work with YDB types and values
  "github.com/ydb-platform/ydb-go-sdk-auth-environ" // needed to authenticate using environment variables
  "github.com/ydb-platform/ydb-go-yc" // to work with YDB in Yandex Cloud
)
```

App code snippet for driver initialization:

```go
ctx := context.Background()
// connection string
dsn := "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g8skpblkos03malf3s/etn01f8gv9an9sedo9fu"
// IAM token
token := "t1.9euelZrOy8aVmZKJm5HGjceMkMeVj-..."
// create a connection object called db, it is an entry point for YDB services
db, err := ydb.Open(ctx,  dsn,
//  yc.WithInternalCA(), // use Yandex Cloud certificates
  ydb.WithAccessTokenCredentials(token), // authenticate using the token
//  ydb.WithAnonimousCredentials(), // authenticate anonymously (for example, using docker ydb)
//  yc.WithMetadataCredentials(token), // authenticate from inside a VM in Yandex Cloud or Yandex Function
//  yc.WithServiceAccountKeyFileCredentials("~/.ydb/sa.json"), // authenticate in Yandex Cloud using a service account file
//  environ.WithEnvironCredentials(ctx), // authenticate using environment variables
)
if err != nil {
  // handle a connection error
}
// driver must be closed when done
defer db.Close(ctx)
```

The `db` object is an input point for working with `YDB` services.
To work with the table service, use the `db.Table()` client.
The client of the table service provides an `API` for making queries to tables.
The most popular method is `db.Table().Do(ctx, op)`. It implements session creation in the background and the repeat attempts to execute the custom `op` operation where the created session is passed to the user's code.
The session has an exhaustive `API` that lets you perform `DDL`, `DML`, `DQL`, and `TCL` requests.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

To create tables, use the `table.Session.CreateTable()` method:

```go
err = db.Table().Do(ctx,
  func(ctx context.Context, s table.Session) (err error) {
    return s.CreateTable(ctx, path.Join(db.Name(), "series"),
      options.WithColumn("series_id", types.TypeUint64),  // not null column
      options.WithColumn("title", types.Optional(types.TypeUTF8)),
      options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
      options.WithColumn("release_date", types.Optional(types.TypeDate)),
      options.WithColumn("comment", types.Optional(types.TypeUTF8)),
      options.WithPrimaryKeyColumn("series_id"),
    )
  },
)
if err != nil {
  // handling query execution failure
}
```

You can use the `table.Session.DescribeTable()` method to print information about the table structure and make sure that it was properly created:

```go
err = db.Table().Do(ctx,
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
  },
)
if err != nil {
  // handling query execution failure
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
err := db.Table().Do(ctx,
  func(ctx context.Context, s table.Session) (err error) {
    var (
      res   result.Result
      id    uint64 // a variable for required results
      title *string // a pointer for optional results
      date  *time.Time // a pointer for optional results
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
      `
,      table.NewQueryParameters(
        table.ValueParam("$seriesID", types.Uint64Value(1)), // insert into the query criteria
      ),
    )
    if err != nil {
      return err
    }
    defer res.Close() // result must be closed
    log.Printf("> select_simple_transaction:\n")
    for res.NextResultSet(ctx) {
      for res.NextRow() {
        // use ScanNamed to pass column names from the scan string,
        // addresses (and data types) to be assigned the query results
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
          id, *title, *date,
        )
      }
    }
    return res.Err()
  },
)
if err != nil {
  // handle a query execution error
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
err = c.Do(ctx,
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
    defer res.Close() // be sure to close the result
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
        // named.OptionalWithDefault enables you to "deploy" optional
        // results or use the default type value in Go
        err = res.ScanNamed(
          named.Required("series_id", &seriesID),
          named.OptionalWithDefault("season_id", &seasonID),
          named.OptionalWithDefault("title", &title),
          named.OptionalWithDefault("first_aired", &date),
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
  // handling a query execution error
}
```
