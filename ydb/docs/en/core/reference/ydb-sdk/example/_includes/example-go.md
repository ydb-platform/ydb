# App in Go

This page contains a detailed description of the code of a [test app](https://github.com/yandex-cloud/ydb-go-sdk/tree/master/example/basic_example_v1) that is available as part of the {{ ydb-short-name }} [Go SDK](https://github.com/yandex-cloud/ydb-go-sdk).

{% include [addition.md](auxilary/addition.md) %}

{% include [init.md](steps/01_init.md) %}

App code snippet for driver initialization:

```go
func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
  dialer := &ydb.Dialer{
    DriverConfig: cmd.config(params),
    TLSConfig:    cmd.tls(),
    Timeout:      time.Second,
  }
  driver, err := dialer.Dial(ctx, params.Endpoint)
  if err != nil {
    return fmt.Errorf("dial error: %v", err)
  }
  defer driver.Close()
```

App code snippet for creating a session:

```go
tableClient := table.Client{
  Driver: driver,
}
sp := table.SessionPool{
  IdleThreshold: time.Second,
  Builder:       &tableClient,
}
defer sp.Close(ctx)
```

{% include [create_table.md](steps/02_create_table.md) %}

To create tables, use the `Session.CreateTable()` method:

```go
func createTables(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
  err = table.Retry(ctx, sp,
    table.OperationFunc(func(ctx context.Context, s *table.Session) error {
      return s.CreateTable(ctx, path.Join(prefix, "series"),
        table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
        table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
        table.WithColumn("series_info", ydb.Optional(ydb.TypeUTF8)),
        table.WithColumn("release_date", ydb.Optional(ydb.TypeUint64)),
        table.WithColumn("comment", ydb.Optional(ydb.TypeUTF8)),
        table.WithPrimaryKeyColumn("series_id"),
      )
    }),
  )
```

You can use the `Session.DescribeTable()` method to output information about the table structure and make sure that it was properly created:

```go
func describeTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
  err = table.Retry(ctx, sp,
    table.OperationFunc(func(ctx context.Context, s *table.Session) error {
      desc, err := s.DescribeTable(ctx, path)
      if err != nil {
        return err
      }
      log.Printf("\n> describe table: %s", path)
      for _, c := range desc.Columns {
        log.Printf("column, name: %s, %s", c.Type, c.Name)
      }
      return nil
    }),
  )
```

{% include [pragmatablepathprefix.md](auxilary/pragmatablepathprefix.md) %}

{% include [query_processing.md](steps/03_query_processing.md) %}

To execute YQL queries, use the `Session.Execute()` method.
The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the ```TxControl``` class.

```go
func selectSimple(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
  query := render(
    template.Must(template.New("").Parse(`
      PRAGMA TablePathPrefix("not_var{{ .TablePathPrefix }}");
      DECLARE $seriesID AS Uint64;
      $format = DateTime::Format("%Y-%m-%d");
      SELECT
        series_id,
        title,
        $format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
      FROM
        series
      WHERE
        series_id = $seriesID;
    `)),
    templateConfig{
      TablePathPrefix: prefix,
    },
  )
  readTx := table.TxControl(
    table.BeginTx(
      table.WithOnlineReadOnly(),
    ),
    table.CommitTx(),
  )
  var res *table.Result
  err = table.Retry(ctx, sp,
    table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
      _, res, err = s.Execute(ctx, readTx, query,
        table.NewQueryParameters(
          table.ValueParam("$seriesID", ydb.Uint64Value(1)),
        ),
        table.WithQueryCachePolicy(
          table.WithQueryCachePolicyKeepInCache(),
        ),
        table.WithCollectStatsModeBasic(),
      )
      return
    }),
  )
  if err != nil {
    return err
  }
  for res.NextSet() {
    for res.NextRow() {
      res.SeekItem("series_id")
      id := res.OUint64()

      res.NextItem()
      title := res.OUTF8()

      res.NextItem()
      date := res.OString()

      log.Printf(
        "\n> select_simple_transaction: %d %s %s",
        id, title, date,
      )
    }
  }
  if err := res.Err(); err != nil {
    return err
  }
  return nil
}
```

{% include [results_processing.md](steps/04_results_processing.md) %}

Query results:

```go
for res.NextSet() {
  for res.NextRow() {
    res.SeekItem("series_id")
    id := res.OUint64()

    res.NextItem()
    title := res.OUTF8()

    res.NextItem()
    date := res.OString()

    log.Printf(
      "\n> select_simple_transaction: %d %s %s",
      id, title, date,
    )
  }
}
```

{% include [scan_query.md](steps/08_scan_query.md) %}

```go
func executeScanQuery(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
query := `
  SELECT series_id, season_id, COUNT(*) AS episodes_count
  FROM episodes
  GROUP BY series_id, season_id
  ORDER BY series_id, season_id;`

var res *table.Result
err = table.Retry(ctx, sp,
  table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
    res, err = s.StreamExecuteScanQuery(ctx, query, table.NewQueryParameters())
    return err
  }),
)
if err != nil {
  return err
}
var (
  seriesID uint64
  seasonID uint64
  count    uint64
)
log.Print("\n> scan_query_select:")
for res.NextResultSet(ctx, "series_id", "season_id", "episodes_count") {
  for res.NextRow() {
    err = res.ScanWithDefaults(&seriesID, &seasonID, &count)
    if err != nil {
      return err
    }
    log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Count: %d", seriesID, seasonID, count)
  }
}
if err = res.Err(); err != nil {
  return err
}
return nil
}
```

{% include [error_handling.md](steps/50_error_handling.md) %}

