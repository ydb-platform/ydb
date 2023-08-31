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

  {% include [auth-static-with-native](../../../../_includes/go/auth-static-with-native.md) %}

- Go (database/sql)

  You can pass the username and password in the connection string. For example:

  {% include [auth-static-database-sql](../../../../_includes/go/auth-static-database-sql.md) %}

  You can also explicitly pass the username and password at driver initialization via a connector using the `ydb.WithStaticCredentials` parameter:

  {% include [auth-static-with-database-sql](../../../../_includes/go/auth-static-with-database-sql.md) %}

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

- Node.js

  {% include [auth-static](../../../../_includes/nodejs/auth-static.md) %}

- PHP

  {% include [feature is not implemented](_includes/wip.md) %}

{% endlist %}
