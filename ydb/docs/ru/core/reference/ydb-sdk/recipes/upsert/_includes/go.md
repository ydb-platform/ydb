```go
package main

import (
	"context"
	"os"
	
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
	defer func() { 
		_ = db.Close(ctx) 
	}()
	// execute bulk upsert with native ydb data
	err = db.Table().DoTx( // Do retry operation on errors with best effort
		ctx, // context manages exiting from Do
		func(ctx context.Context, tx table.TransactionActor) (err error) { // retry operation
			res, err = tx.Execute(ctx, `
					PRAGMA TablePathPrefix("/path/to/table");
					DECLARE $seriesID AS Uint64;
					DECLARE $seasonID AS Uint64;
					DECLARE $episodeID AS Uint64;
					DECLARE $views AS Uint64;
					UPSERT INTO episodes ( series_id, season_id, episode_id, views )
					VALUES ( $seriesID, $seasonID, $episodeID, $views );
				`,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", types.Uint64Value(1)),
					table.ValueParam("$seasonID", types.Uint64Value(1)),
					table.ValueParam("$episodeID", types.Uint64Value(1)),
					table.ValueParam("$views", types.Uint64Value(1)), // increment views
				),
			)
			if err != nil {
				return err
			}
			if err = res.Err(); err != nil {
				return err
			}
			return res.Close()
		},
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
	}
}
```
