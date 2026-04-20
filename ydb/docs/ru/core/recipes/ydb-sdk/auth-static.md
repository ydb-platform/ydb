<!-- markdownlint-disable blanks-around-fences -->

# Аутентификация при помощи логина и пароля

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

- Go

  {% list tabs %}

  - Native SDK

    Передать логин и пароль можно в составе строки подключения. Например, так:

    ```shell
    "grpcs://login:password@localhost:2135/local"
    ```

    Также можно передать логин и пароль явно через опцию `ydb.WithStaticCredentials`:

    {% include [auth-static-with-native](../../../_includes/go/auth-static-with-native.md) %}

  - database/sql

    Передать логин и пароль можно в составе строки подключения. Например, так:

    {% include [auth-static-database-sql](../../../_includes/go/auth-static-database-sql.md) %}

    Также можно передать логин и пароль явно при инициализации драйвера через коннектор с помощью специальной опции `ydb.WithStaticCredentials`:

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

        // Логин и пароль могут быть указаны напрямую
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", username, password)) {
            doWork(connection);
        }
    }
    ```

    В Spring Boot, ORM и прочих сторонних фреймворках вокруг JDBC задайте те же JDBC URL, логин и пароль, что и в примере выше (например, `spring.datasource.url`, `spring.datasource.username`, `spring.datasource.password` или эквивалент в конфигурации пула).

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
