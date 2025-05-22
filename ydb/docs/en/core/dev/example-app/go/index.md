# Example app in Go

<!-- markdownlint-disable blanks-around-fences -->

This page provides a detailed description of the code for a [test app](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic) that uses the {{ ydb-short-name }} [Go SDK](https://github.com/ydb-platform/ydb-go-sdk).

## Downloading and starting {#download}

The instructions below assume that [Git](https://git-scm.com/downloads) and [Go](https://go.dev/doc/install) are installed. Make sure to install the [YDB Go SDK](../../../reference/ydb-sdk/install.md).

Create a working directory and use it to run the following command from the command line to clone the GitHub repository:

```bash
git clone https://github.com/ydb-platform/ydb-go-sdk.git
```

Next, from the same working directory, run the following command to start the test app:

{% include [run_options.md](_includes/run_options.md) %}

{% include [init.md](../_includes/steps/01_init.md) %}

To work with {{ ydb-short-name }} in `Go`, import the `ydb-go-sdk` driver package:

```go
import (
 "context"
 "log"
 "path"

 "github.com/ydb-platform/ydb-go-sdk/v3"
 "github.com/ydb-platform/ydb-go-sdk/v3/query"
)
```

To interact with {{ ydb-short-name }}, it is necessary to create a {{ ydb-short-name }} driver:

```go
db, err := ydb.Open(context.Background(), "grpc://localhost:2136/local")
if err != nil {
  // handle connection error
}

// You should close the driver when your application finishes its work (for example, when exiting the program).
defer db.Close(context.Background())
```

The `ydb.Open` method returns a driver instance if successful. The driver performs several services, such as {{ ydb-short-name }} cluster discovery and client-side load balancing.

The `ydb.Open` method takes two mandatory arguments:

* a context
* a {{ ydb-short-name }} connection string

There are also many connection options available that let you override the default settings.


By default, anonymous authentication is used. To connect to the {{ ydb-short-name }} cluster using a token, use the following syntax:

```go
db, err := ydb.Open(context.Background(), clusterEndpoint,
 ydb.WithAccessTokenCredentials(token),
)
```
You can see the full list of auth providers in the [ydb-go-sdk documentation](https://github.com/ydb-platform/ydb-go-sdk?tab=readme-ov-file#credentials-) and on the [recipes page](../../../recipes/ydb-sdk/auth.md).

It is necessary to close the driver at the end of work to clean up resources.

```go
defer db.Close(ctx)
```

The `db` struct is the entry point for working with {{ ydb-short-name }}. To query tables, use the `db.Query()` query service:

YQL queries are executed within special objects called `query.Session`. Sessions store the execution context of queries (for example, transactions) and provide server-side load balancing among the {{ ydb-short-name }} cluster nodes.

The query service client provides an API for executing queries:

* `db.Query().Do(ctx, op)` creates sessions in the background and automatically retries the provided `op func(ctx context.Context, s query.Session) error` operation if necessary. As soon as a session is ready, it is passed to the callback.
* `db.Query().DoTx(ctx, op)` automatically handles the transaction lifecycle. It provides a prepared transaction object, `query.TxActor`, to the user-defined function `op func(ctx context.Context, tx query.TxActor)` error. If the operation returns without an error (nil), the transaction commits automatically. If the operation returns an error, the transaction rolls back automatically.
* `db.Query().Exec` runs a single query that returns **no result**, with automatic retry logic on failure. This method returns nil if the execution is successful or an error otherwise.
* `db.Query().Query` executes a single query containing one or more statements that return a result. It automatically handles retries. Upon successful execution, it returns a fully materialized result (`query.Result`). All result rows are loaded into memory and available for immediate iteration. For queries returning large datasets, this may lead to an [out of memory](https://en.wikipedia.org/wiki/Out_of_memory) problem.
* `db.Query().QueryResultSet` executes a query that contains exactly one statement returning results (it may contain other auxiliary statements that return no results, such as `UPSERT`). Like `db.Query().Query`, it automatically retries failed operations and returns a fully materialized result set (`query.ResultSet`). Queries that return large datasets may cause an [OOM](https://en.wikipedia.org/wiki/Out_of_memory) error.
* `db.Query().QueryRow` runs queries expected to return exactly one row. It also automatically retries failed operations. On success, it returns a `query.Row` instance.

{% include [steps/02_create_table.md](../_includes/steps/02_create_table.md) %}

Example of a query with no returned result (table creation):

```go
import "github.com/ydb-platform/ydb-go-sdk/v3/query"

err = db.Query().Exec(ctx, `
 CREATE TABLE IF NOT EXISTS series (
  series_id Bytes,
  title Text,
  series_info Text,
  release_date Date,
  comment Text,

  PRIMARY KEY(series_id)
 )`, query.WithTxControl(query.NoTx()),
)
if err != nil {
  // handle query execution error
}
```

{% include [steps/04_query_processing.md](../_includes/steps/04_query_processing.md) %}

To execute YQL queries and fetch results, use `query.Session` methods: `query.Session.Query`, `query.Session.QueryResultSet`, or `query.Session.QueryRow`.

The {{ ydb-short-name }} SDK supports explicit transaction control via the `query.TxControl` structure:

```go
readTx := query.TxControl(
 query.BeginTx(
  query.WithSnapshotReadOnly(),
 ),
 query.CommitTx(),
)
row, err := db.Query().QueryRow(ctx,`
 DECLARE $seriesID AS Uint64;
 SELECT
   series_id,
   title,
   release_date
 FROM
   series
 WHERE
   series_id = $seriesID;`,
 query.WithParameters(
  ydb.ParamsBuilder().Param("$seriesID").Uint64(1).Build(),
 ),
 query.WithTxControl(readTx),
)
if err != nil {
  // handle query execution error
}
```

You can extract row data (`query.Row`) using the following methods:

* `query.Row.ScanStruct` — scans row data into a struct based on struct field tags that match column names.
* `query.Row.ScanNamed` — scans data into variables using explicitly defined column-variable pairs.
* `query.Row.Scan` — scans data directly by column order into the provided variables.

{% list tabs %}

- ScanStruct

  ```go
  var info struct {
   SeriesID    string    `sql:"series_id"`
   Title       string    `sql:"title"`
   ReleaseDate time.Time `sql:"release_date"`
  }
  err = row.ScanStruct(&info)
  if err != nil {
    // handle query execution error
  }
  ```

- ScanNamed

  ```go
  var seriesID, title string
  var releaseDate time.Time
  err = row.ScanNamed(query.Named("series_id", &seriesID), query.Named("title", &title), query.Named("release_date", &releaseDate))
  if err != nil {
    // handle query execution error
  }
  ```

- Scan

  ```go
  var seriesID, title string
  var releaseDate time.Time
  err = row.Scan(&seriesID, &title, &releaseDate)
  if err != nil {
    // handle query execution error
  }
  ```
  
 {% endlist %}

{% include [scan_query.md](../_includes/steps/08_scan_query.md) %}

{% note warning %}

If the expected query result is very large, avoid loading all data into memory using helper methods like `query.Client.Query` or `query.Client.QueryResultSet`. These methods return fully materialized results, storing all rows from the server in local client memory. Large result sets can cause an [OOM](https://en.wikipedia.org/wiki/Out_of_memory) problem.

Instead, use the `query.TxActor.Query` or `query.TxActor.QueryResultSet` methods on a transaction or session. These methods return iterators over results without fully materializing them upfront. The `query.Session` object is accessible via the `query.Client.Do` method, which handles automatic retries. Keep in mind that the read operation can be interrupted at any time, restarting the entire query process. Therefore, the user function passed to `Do` may run multiple times.

{% endnote %}


```go
err = db.Query().Do(ctx,
 func(ctx context.Context, s query.Session) error {
  rows, err := s.QueryResultSet(ctx,`
   SELECT series_id, season_id, title, first_aired
   FROM seasons`,
  )
  if err != nil {
   return err
  }
  defer rows.Close(ctx)
  for row, err := range rows.Rows(ctx) {
   if err != nil {
    return err
   }
   var info struct {
    SeriesID    string    `sql:"series_id"`
    SeasonID    string    `sql:"season_id"`
    Title       string    `sql:"title"`
    FirstAired  time.Time `sql:"first_aired"`
   }
   err = row.ScanStruct(&info)
   if err != nil {
    return err
   }
   fmt.Printf("%+v\n", info)
  }
  return nil
 },
 query.WithIdempotent(),
)
if err != nil {
  // handle query execution error
}
```
