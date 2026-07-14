<!-- markdownlint-disable blanks-around-fences -->

# Authentication using login and password

Static authentication (login and password) passes the `user`/`password` pair when connecting to {{ ydb-short-name }}. This method is used on dedicated installations of {{ ydb-short-name }} where login/password authentication is enabled. Typical steps: set a login and password, create a static credentials provider, open a transport, and execute a query. For details, see the [Authentication](../../reference/ydb-sdk/auth.md) section; for basic connection, see the [driver initialization](./init.md) recipe. Other methods: [token](./auth-access-token.md), [anonymous](./auth-anonymous.md), [environment variables](./auth-env.md), [metadata](./auth-metadata.md), [service account](./auth-service-account.md).

Below are code examples for authentication using a login and password in different {{ ydb-short-name }} SDKs.

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

    The code for initializing `ydb::YdbComponent`, obtaining `ydb::TableClient`, and starting `components::MinimalServerComponentList` is as in the example from [init.md](./init.md).

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

    You can also pass the login and password explicitly when initializing the driver through a connector using a special `ydb.WithStaticCredentials` option:

    {% include [auth-static-with-database-sql](../../../_includes/go/auth-static-with-database-sql.md) %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.auth.StaticCredentials;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class StaticAuthExample {
        public static void main(String[] args) throws Exception {
            // Connection string from environment variable or local {{ ydb-short-name }} by default
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            String username = System.getenv("YDB_USER");
            String password = System.getenv("YDB_PASSWORD");
            if (username == null || username.isEmpty() || password == null) {
                throw new IllegalStateException("Задайте переменные окружения YDB_USER и YDB_PASSWORD");
            }

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(new StaticCredentials(username, password))
                    .build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                QueryReader reader = retryCtx.supplyResult(
                        session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.NONE))
                ).join().getValue();

                // Connection check: output the result of SELECT 1
                ResultSetReader rs = reader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getColumn(0).getInt32());
                }
            }
        }
    }
    ```

  - JDBC

    ```java
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.Properties;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class StaticAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            String username = System.getenv("YDB_USER");
            String password = System.getenv("YDB_PASSWORD");
            if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
                throw new IllegalStateException("Задайте переменные окружения YDB_USER и YDB_PASSWORD");
            }

            Properties props = new Properties();
            props.setProperty("username", username);
            props.setProperty("password", password);

            try (Connection connection = DriverManager.getConnection(jdbcUrl, props);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```


    You can also pass the login and password as the second and third arguments of the `DriverManager.getConnection(jdbcUrl, username, password)` method.

    In Spring Boot, ORM, and other third-party frameworks around JDBC, set the same JDBC URL, login, and password as in the example above (for example, `spring.datasource.url`, `spring.datasource.username`, `spring.datasource.password` or an equivalent in the pool configuration).

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
