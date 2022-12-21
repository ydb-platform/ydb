# Authentication using a service account file

{% include [work in progress message](_includes/addition.md) %}

Below are examples of the code for authentication using a service account file in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     yc "github.com/ydb-platform/ydb-go-yc"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     db, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       yc.WithServiceAccountKeyFileCredentials(
         os.Getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"),
       ),
       yc.WithInternalCA(), // append Yandex Cloud certificates
     )
     if err != nil {
       panic(err)
     }
     defer db.Close(ctx)
     ...
   }
   ```

- Go (database/sql)

   ```go
   package main

   import (
     "context"
     "database/sql"
     "os"

     "github.com/ydb-platform/ydb-go-sdk/v3"
     yc "github.com/ydb-platform/ydb-go-yc"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       yc.WithServiceAccountKeyFileCredentials(
         os.Getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"),
       ),
       yc.WithInternalCA(), // append Yandex Cloud certificates
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
  public void work(String connectionString, String saKeyPath) {
      AuthProvider authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyPath);

      GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
              .withAuthProvider(authProvider)
              .build());
      
      TableClient tableClient = TableClient.newClient(transport).build();

      doWork(tableClient);

      tableClient.close();
      transport.close();
  }
  ```

- Node.js
  
  ### Load service account data from file

  {% include [auth-sa-file](../../../../_includes/nodejs/auth-sa-file.md) %}

  ### Load service account data from other source

  When you need to load service account data from the outside, for example, from the secrets vault, this method is suitable:

  {% include [service-account-data](../../../../_includes/nodejs/auth-sa-data.md) %}

{% endlist %}
