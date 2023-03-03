# StaleReadOnly

{% list tabs %}

- Go (native)

  ```go
  package main

  import (
    "context"
    "os"
    
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/table"
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
    txControl := table.TxControl(
      table.BeginTx(table.WithStaleReadOnly()),
      table.CommitTx(),
    )
    err := driver.Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
      _, _, err := s.Execute(ctx, txControl, "SELECT 1", nil)
      return err
    })
    if err != nil {
      fmt.Printf("unexpected error: %v", err)
    }
  }
  ```


{% endlist %}
