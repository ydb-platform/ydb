# Authentication using a token

{% include [work in progress message](_includes/addition.md) %}

Below are examples of the code for authentication using a token in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
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
     ...
   }
   ```

- Go (database/sql)

   {% cut "If you use a connector to create a connection to {{ ydb-short-name }}" %}

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
       ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
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

   {% endcut %}

   {% cut "If you use a connection string" %}

   ```go
   package main

   import (
     "context"
     "database/sql"
     "os"

     _ "github.com/ydb-platform/ydb-go-sdk/v3"
   )

   func main() {
     db, err := sql.Open("ydb", "grpcs://localohost:2135/local?token="+os.Getenv("YDB_TOKEN"))
     if err != nil {
       panic(err)
     }
     defer db.Close()
     ...
   }
   ```

   {% endcut %}


- Java

   ```java
   public void work(String connectionString, String accessToken) {
       AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
           IamTokenCredentialProvider.builder()
               .token(accessToken)
               .build()
       );

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

  {% include [access-token](../../../../_includes/nodejs/recipes/auth/access-token.md) %}

{% endlist %}
