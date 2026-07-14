# Authentication using a token

<!-- markdownlint-disable blanks-around-fences -->

Authentication with an access token passes a pre-obtained IAM token to each request to {{ ydb-short-name }}. This method is suitable when the token is already available in the application — for example, obtained via OAuth or the `yc` utility. Typical steps: get a token, create an authentication provider, open a transport, and execute a query. For details on authentication modes, see the [Authentication](../../reference/ydb-sdk/auth.md) section; for basic connection, see the [driver initialization](./init.md) recipe. Alternatives: [anonymous](./auth-anonymous.md), [environment variables](./auth-env.md), [metadata](./auth-metadata.md), [service account](./auth-service-account.md), [login and password](./auth-static.md).

Below are code examples for authentication using a token in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    NYdb::TDriver CreateDriverWithAccessToken(const std::string& accessToken) {
        auto config = NYdb::TDriverConfig("grpcs://localhost:2135/?database=/local")
            .SetAuthToken(accessToken);

        return NYdb::TDriver(config);
    }
    ```

  - userver

    {% cut "secdist" %}

    ```json
    {
      "ydb_settings": {
        "db": {
          "token": "<access token>"
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

  - database/sql

    {% cut "If used connector for creating connection to {{ ydb-short-name }}" %}

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

    {% cut "If used string connection" %}

    ```go
    package main

    import (
      "context"
      "database/sql"
      "os"

      _ "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      db, err := sql.Open("ydb", "grpcs://localhost:2135/local?token="+os.Getenv("YDB_TOKEN"))
      if err != nil {
        panic(err)
      }
      defer db.Close()
      ...
    }
    ```

    {% endcut %}

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.auth.TokenAuthProvider;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class AccessTokenExample {
        public static void main(String[] args) throws Exception {
            // Connection string from environment variable or local {{ ydb-short-name }} by default
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // IAM token from environment variable YDB_ACCESS_TOKEN_CREDENTIALS
            String token = System.getenv("YDB_ACCESS_TOKEN_CREDENTIALS");
            if (token == null || token.isEmpty()) {
                throw new IllegalStateException("Задайте переменную окружения YDB_ACCESS_TOKEN_CREDENTIALS");
            }

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(new TokenAuthProvider(token))
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

    public class AccessTokenJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Option 1: token is set by the token property
            Properties propsWithToken = new Properties();
            String token = System.getenv("YDB_ACCESS_TOKEN_CREDENTIALS");
            if (token == null || token.isEmpty()) {
                throw new IllegalStateException("Задайте переменную окружения YDB_ACCESS_TOKEN_CREDENTIALS");
            }
            propsWithToken.setProperty("token", token);

            try (Connection connection = DriverManager.getConnection(jdbcUrl, propsWithToken);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }

            // Option 2: token is read from a file — the tokenFile property
            Properties propsWithTokenFile = new Properties();
            propsWithTokenFile.setProperty("tokenFile", System.getenv().getOrDefault("YDB_TOKEN_FILE", "~/.ydb_token"));

            try (Connection connection = DriverManager.getConnection(jdbcUrl, propsWithTokenFile);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```


    In Spring Boot, ORM, and other third-party frameworks around JDBC, use the same JDBC connection string and the same authentication parameters as above (for example, `spring.datasource.url` with query parameters or `spring.datasource.*` for the token and token file).

  {% endlist %}

- JavaScript

  {% include [auth-access-token](../../_includes/nodejs/auth-access-token.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-access-token](../../_includes/python/auth-access-token.md) %}

  - Native SDK (Asyncio)

    {% include [auth-access-token](../../_includes/python/async/auth-access-token.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": {"token": os.environ["YDB_TOKEN"]}
        }
    )
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  ```C#
  using Ydb.Sdk.Ado;
  using Ydb.Sdk.Auth;

  var builder = new YdbConnectionStringBuilder("Host=localhost;Port=2136;Database=/local")
  {
      CredentialsProvider = new TokenProvider("MY_VERY_SECURE_TOKEN")
  };

  await using var dataSource = new YdbDataSource(builder);
  await using var connection = await dataSource.OpenConnectionAsync();
  ```


  Not supported for Entity Framework and linq2db.

- Rust

  ```rust
  use ydb::{AccessTokenCredentials, ClientBuilder, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(AccessTokenCredentials::from(std::env::var("YDB_TOKEN")?))
      .client()?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\Implement\AccessTokenAuthentication;

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

      'credentials' => new AccessTokenAuthentication('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
