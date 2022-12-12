# Anonymous authentication

{% include [work in progress message](_includes/addition.md) %}

Below are examples of the code for anonymous authentication in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

   By default, anonymous authentication is used.
   You can explicitly enable anonymous authentication as follows:
   ```go
   package main

   import (
     "context"

     "github.com/ydb-platform/ydb-go-sdk/v3"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     db, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithAnonymousCredentials(),
     )
     if err != nil {
       panic(err)
     }
     defer db.Close(ctx)
     ...
   }
   ```

- Go (database/sql)

   By default, anonymous authentication is used.
   You can explicitly enable anonymous authentication as follows:
   ```go
   package main

   import (
     "context"
     "database/sql"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       ydb.WithAnonymousCredentials(),
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

- Java

   ```java
   public void work(String connectionString) {
       AuthProvider authProvider = NopAuthProvider.INSTANCE;

       GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
               .withAuthProvider(authProvider)
               .build();

       TableClient tableClient = TableClient
           .newClient(GrpcTableRpc.ownTransport(transport))
           .build());

       doWork(tableClient);

       tableClient.close();
   }
   ```

- Node.js

  {% include [anonymous](../../../../_includes/nodejs/recipes/auth/anonymous.md) %}

{% endlist %}
