```go
package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func main() {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		metrics.WithTraces(
			registry,
			metrics.WithDetails(trace.DetailsAll),
			metrics.WithSeparator("_"),
		),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
}
```
