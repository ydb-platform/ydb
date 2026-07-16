<!-- markdownlint-disable blanks-around-fences -->

# Authentication using login and password

Below are code examples for authentication using login and password in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/types/credentials/credentials.h>

    NYdb::TDriver CreateDriverWithStaticCredentials(
        const std::string& connectionString,
        const std::string& user,
        const std::string& password)
    {
        auto config = NYdb::TDriverConfig(connectionString)
            .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
                .User = user,
                .Password = password,
            }));

        return NYdb::TDriver(config);
    }
    ```

  - userver

    {% cut "secdist" %}

    ```json
    {
      "ydb_settings": {
        "db": {
          "user": "user",
          "password": "password"
        }
      }
    }
    ```

    {% endcut %}

    The code for initializing `ydb::YdbComponent`, obtaining `ydb::TableClient`, and starting `components::MinimalServerComponentList` is the same as in the example from [init.md](./init.md).

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

    You can pass the login and password as part of the connection string. For example:


    ```shell
    "grpcs://login:password@localhost:2135/local"
    ```


    You can also pass the login and password explicitly via the `ydb.WithStaticCredentials` option:

    {% include [auth-static-with-native](../../../_includes/go/auth-static-with-native.md) %}

  - database/sql

    You can pass the login and password as part of the connection string. For example:

    {% include [auth-static-database-sql](../../../_includes/go/auth-static-database-sql.md) %}

    You can also pass the login and password explicitly during driver initialization via a connector using a special option `ydb.WithStaticCredentials`:

    {% include [auth-static-with-database-sql](../../../_includes/go/auth-static-with-database-sql.md) %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    public void work(String connectionString, String username, String password) {
        StaticCredentials authProvider = new StaticCredentials(username, password);

        try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();
             QueryClient queryClient = QueryClient.newClient(transport).build()) {

            doWork(queryClient);
        }
    }
    ```

  - JDBC

    ```java
    public void work(String username, String password) throws SQLException {
        Properties props = new Properties();
        props.setProperty("username", username);
        props.setProperty("password", password);
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props)) {
            doWork(connection);
        }

        // Login and password can be specified directly
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", username, password)) {
            doWork(connection);
        }
    }
    ```


    In Spring Boot, ORM, and other third-party frameworks around JDBC, set the same JDBC URL, login, and password as in the example above (for example, `spring.datasource.url`, `spring.datasource.username`, `spring.datasource.password` or their equivalent in the pool configuration).

  {% endlist %}

- JavaScript

  {% include [auth-static](../../_includes/nodejs/auth-static.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-static](../../_includes/python/auth-static.md) %}

  - Native SDK (Asyncio)

    {% include [auth-static](../../_includes/python/async/auth-static.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": {
                "username": os.environ["YDB_USER"],
                "password": os.environ["YDB_PASSWORD"]
            }
        }
    )
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource(
      "Host=localhost;Port=2136;Database=/local;User=user;Password=password");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

- Rust

  ```rust
  use ydb::{ClientBuilder, StaticCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(StaticCredentials::new(
          std::env::var("YDB_USER")?,
          std::env::var("YDB_PASSWORD")?,
          http::Uri::from_static("grpc://localhost:2136"),
          "local".into(),
      ))
      .client()?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\Implement\StaticAuthentication;

  $config = [

      // Database path
      'database'    => '/local',

      // Database endpoint
      'endpoint'    => 'localhost:2136',

      // Auto discovery (dedicated server only)
      'discovery'   => false,

      // IAM config
      'iam_config'  => [
          'insecure' => true,
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)
      ],

      'credentials' => new StaticAuthentication($user, $password)
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
