# Аутентификация при помощи логина и пароля

{% include [work in progress message](_includes/addition.md) %}

Ниже приведены примеры кода аутентификации при помощи логина и пароля в разных {{ ydb-short-name }} SDK.

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

  Передать логин и пароль можно в составе строки подключения. Например, так:

  ```shell
  "grpcs://login:password@localohost:2135/local"
  ```

  Также можно передать логин и пароль явно через опцию `ydb.WithStaticCredentials`:

  {% include [auth-static-with-native](../../../../_includes/go/auth-static-with-native.md) %}

- Go (database/sql)

  Передать логин и пароль можно в составе строки подключения. Например, так:

  {% include [auth-static-database-sql](../../../../_includes/go/auth-static-database-sql.md) %}

  Также можно передать логин и пароль явно при инициализации драйвера через коннектор с помощью специальной опции `ydb.WithStaticCredentials`:

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

- Python

  {% include [auth-static](../../../../_includes/python/auth-static.md) %}

- Python (asyncio)

  {% include [auth-static](../../../../_includes/python/async/auth-static.md) %}

- PHP

  {% include [feature is not implemented](_includes/wip.md) %}

{% endlist %}
