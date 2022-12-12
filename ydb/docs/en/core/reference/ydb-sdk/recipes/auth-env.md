# Authentication using environment variables

{% include [work in progress message](_includes/addition.md) %}

When using this method, the authentication mode and its parameters are defined by the environment that an application is run in, [as described here](../auth.md#env).

By setting one of the following environment variables, you can control the authentication method:

* `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path/to/sa_key_file>`: Use a service account file in Yandex Cloud.
* `YDB_ANONYMOUS_CREDENTIALS="1"`: Use anonymous authentication. Relevant for testing against a Docker container with {{ ydb-short-name }}.
* `YDB_METADATA_CREDENTIALS="1"`: Use the metadata service inside Yandex Cloud (a Yandex function or a VM).
* `YDB_ACCESS_TOKEN_CREDENTIALS=<access_token>`: Use token-based authentication.

Below are examples of the code for authentication using environment variables in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- Go (native)

   ```go
   package main

   import (
     "context"
     "os"

     environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
     "github.com/ydb-platform/ydb-go-sdk/v3"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     db, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       environ.WithEnvironCredentials(ctx),
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

     environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
     "github.com/ydb-platform/ydb-go-sdk/v3"
   )

   func main() {
     ctx, cancel := context.WithCancel(context.Background())
     defer cancel()
     nativeDriver, err := ydb.Open(ctx,
       os.Getenv("YDB_CONNECTION_STRING"),
       environ.WithEnvironCredentials(ctx),
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
       AuthProvider authProvider = CloudAuthHelper.getAuthProviderFromEnviron();

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

  {% include [env](../../../../_includes/nodejs/recipes/auth/env.md) %}

{% endlist %}
