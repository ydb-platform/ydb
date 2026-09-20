# Anonymous authentication

<!-- markdownlint-disable blanks-around-fences -->

Anonymous authentication does not transmit credentials — {{ ydb-short-name }} SDK connects to the database without a token, login, or password. This is the default mode for local development and testing against a Docker container with {{ ydb-short-name }}. Typical steps: specify the connection string, explicitly or implicitly select the anonymous provider, and execute a query. Details are in the [Authentication](../../reference/ydb-sdk/auth.md) section; basic connection is in the [driver initialization](./init.md) recipe. Other methods: [token](./auth-access-token.md), [environment variables](./auth-env.md), [metadata](./auth-metadata.md), [service account](./auth-service-account.md), [login and password](./auth-static.md).

Below are code examples of anonymous authentication in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    Anonymous authentication is the default authentication.
    You can explicitly enable anonymous authentication as follows:


    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/types/credentials/credentials.h>

    NYdb::TDriver CreateDriverAnonymous() {
        auto config = NYdb::TDriverConfig("grpc://localhost:2136/local")
            .SetCredentialsProviderFactory(NYdb::CreateInsecureCredentialsProviderFactory());

        return NYdb::TDriver(config);
    }
    ```

  - userver

    If you do not set `credentials-provider` in the static config, do not specify `databases.*.credentials`, and do not place `token`, `iam_jwt_params` and the pair `user`/`password` in secdist for this database, the driver will use the anonymous mode by default.

    Initialization code `ydb::YdbComponent`, obtaining `ydb::TableClient` and starting `components::MinimalServerComponentList` — as in the example from [init.md](./init.md).

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

    Anonymous authentication is the default authentication.
    You can explicitly enable anonymous authentication as follows:


    ```go
    package main

    import (
      "context"

      "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel()
      db, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithAnonymousCredentials(),
      )
      if err != nil {
        panic(err)
      }
      defer db.Close(ctx)
      ...
    }
    ```

  - database/sql

    Anonymous authentication is the default authentication.
    You can explicitly enable anonymous authentication as follows:


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
        ydb.WithAnonymousCredentials(),
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

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.auth.NopAuthProvider;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class AnonymousAuthExample {
        public static void main(String[] args) throws Exception {
            // Connection string from environment variable or local {{ ydb-short-name }} by default
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    // Explicitly specify anonymous authentication (default mode)
                    .withAuthProvider(NopAuthProvider.INSTANCE)
                    .build();
                 QueryClient queryClient = QueryClient.newClient(transport).build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
                QueryReader reader = retryCtx.supplyResult(
                        session -> QueryReader.readFrom(session.createQuery("SELECT 1", TxMode.NONE))
                ).join().getValue();

                // Connection check: output the result SELECT 1
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
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class AnonymousAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Connection without additional properties — anonymous authentication
            try (Connection connection = DriverManager.getConnection(jdbcUrl);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```


    In Spring Boot, ORM, and other third-party frameworks around JDBC, the connection is set with the same JDBC connection string as above (for example, `spring.datasource.url`).

  {% endlist %}

- JavaScript

  {% include [auth-anonymous](../../_includes/nodejs/auth-anonymous.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-anonymous](../../_includes/python/auth-anonymous.md) %}

  - Native SDK (Asyncio)

    {% include [auth-anonymous](../../_includes/python/async/auth-anonymous.md) %}

  - SQLAlchemy

    ```python
    import sqlalchemy as sa

    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```


  For Entity Framework and linq2db, use the same connectionString.

- Rust

  ```rust
  use ydb::{AnonymousCredentials, ClientBuilder, YdbResult};

  let client = ClientBuilder::new_from_connection_string("grpc://localhost:2136?database=local")?
      .with_credentials(AnonymousCredentials::new())
      .client()?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\Implement\AnonymousAuthentication;

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

      'credentials' => new AnonymousAuthentication()
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
