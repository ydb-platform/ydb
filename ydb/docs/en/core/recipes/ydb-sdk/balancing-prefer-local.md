# Prefer the nearest data center

{% include [work in progress message](_includes/addition.md) %}

Below are examples of the code for setting the "prefer the nearest data center" balancing algorithm option in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     db, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithBalancer(
         balancers.PreferLocalDC(
           balancers.RandomChoice(),
         ),
       ),
     )
     if err != nil {
       panic(err)
     }
     defer db.Close(ctx)
     ...
   }
   ```

- Go (database/sql)

   Client load balancing in the {{ ydb-short-name }} `database/sql` driver is performed only when establishing a new connection (in `database/sql` terms), which is a {{ ydb-short-name }} session on a specific node. Once the session is created, all queries in this session are passed to the node where the session was created. Queries in the same {{ ydb-short-name }} session are not balanced between different {{ ydb-short-name }} nodes.

   Example of the code for setting the "prefer the nearest data center" balancing algorithm option:
   ```go
   package main

   import (
     "context"
     "database/sql"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithBalancer(
         balancers.PreferLocalDC(
           balancers.RandomChoice(),
         ),
       ),
     )
     if err != nil {
       panic(err)
     }
     defer nativeDriver.Close(ctx)

     connector, err := ydb.Connector(nativeDriver)
     if err != nil {
       panic(err)
     }

     db := sql.OpenDB(connector)
     defer db.Close()
     ...
   }
   ```

{% endlist %}
