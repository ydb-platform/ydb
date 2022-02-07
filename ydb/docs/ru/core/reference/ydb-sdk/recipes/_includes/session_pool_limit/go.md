```go
package main

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
	db, err := ydb.New(
		ctx,
		...
		ydb.WithSessionPoolSizeLimit(500),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
}
```
