# Username and password based authentication

{% include [work in progress message](_includes/addition.md) %}

Below are examples of the code for authentication based on a username and token in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

   ```c++
   auto driverConfig = NYdb::TDriverConfig()
     .SetEndpoint(endpoint)
     .SetDatabase(database)
     .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
         .User = "user",
         .Password = "password",
     }));

   NYdb::TDriver driver(driverConfig);
   ```

- Go (native)

   You can pass the username and password in the connection string. For example:
   ```shell
   "grpcs://login:password@localohost:2135/local"
   ```

   You can also explicitly pass them using the `ydb.WithStaticCredentials` parameter:
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
         ydb.WithStaticCredentials("user", "password"),
     )
     if err != nil {
         panic(err)
     }
     defer db.Close(ctx)
     ...
   }
   ```

- Go (database/sql)

   You can pass the username and password in the connection string. For example:
   ```go
   package main

   import (
     "context"

     _ "github.com/ydb-platform/ydb-go-sdk/v3"
   )

   func main() {
     db, err := sql.Open("ydb", "grpcs://login:password@localohost:2135/local")
     if err != nil {
         panic(err)
     }
     defer db.Close()
     ...
   }
   ```

   You can also explicitly pass the username and password at driver initialization via a connector using the `ydb.WithStaticCredentials` parameter:
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
     nativeDriver, err := ydb.Open(ctx,
         os.Getenv("YDB_CONNECTION_STRING"),
         ydb.WithStaticCredentials("user", "password"),
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
  public void work(String connectionString, String username, String password) {
      AuthProvider authProvider = new StaticCredentials(username, password);

      GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
              .withAuthProvider(authProvider)
              .build());
      
      TableClient tableClient = TableClient.newClient(transport).build();

      doWork(tableClient);

      tableClient.close();
      transport.close();
  }
  ```


{% endlist %}
