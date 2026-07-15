# Authentication using environment variables

Authentication via environment variables allows you to avoid hard‑coding credentials in code: the SDK or JDBC driver determines the mode itself based on the `YDB_*` variables in the process environment. This method is convenient for containers, CI/CD, and cloud deployment, where secrets are passed through the environment. Typical steps: set the required environment variables, create an authentication provider (or connect via JDBC without explicit properties), and execute a query. Details about the mode selection order are in the [Authentication](../../reference/ydb-sdk/auth.md#env) section; basic connection is in the [driver initialization](./init.md) recipe. Other methods: [token](./auth-access-token.md), [anonymous](./auth-anonymous.md), [metadata](./auth-metadata.md), [service account](./auth-service-account.md), [login and password](./auth-static.md).

When using this method, the authentication mode and its parameters are determined by the environment in which the application runs, in the [order described here](../../reference/ydb-sdk/auth.md#env).

By setting one of the following environment variables, you can control the authentication method:

* `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=<path/to/sa_key_file>` — use a service account file in Yandex Cloud.
* `YDB_ANONYMOUS_CREDENTIALS="1"` — use anonymous authentication. Relevant for testing against a Docker container with {{ ydb-short-name }}.
* `YDB_METADATA_CREDENTIALS="1"` — use the metadata service inside Yandex Cloud (Yandex Function or virtual machine).
* `YDB_ACCESS_TOKEN_CREDENTIALS=<access_token>` — use token authentication.

Below are authentication code examples using environment variables in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/helpers/helpers.h>

    NYdb::TDriver CreateDriverFromEnvironment(const std::string& connectionString) {
        return NYdb::TDriver(NYdb::CreateFromEnvironment(connectionString));
    }
    ```

  - userver

    {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

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

  - database/sql

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

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.auth.iam.CloudAuthHelper;
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;

    public class EnvironAuthExample {
        public static void main(String[] args) throws Exception {
            // Connection string from environment variable or local {{ ydb-short-name }} by default
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // Authentication mode is determined by YDB_* environment variables
            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
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

    The JDBC driver reads the environment variables `YDB_*` in the order described in the [Authentication](../../reference/ydb-sdk/auth.md#env) section. You do not need to pass credentials explicitly — an empty `Properties` object is sufficient.


    ```java
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.Properties;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class EnvironAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            // Empty properties: the driver will choose the authentication method based on environment variables
            try (Connection connection = DriverManager.getConnection(jdbcUrl, new Properties());
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("SELECT 1 = " + rs.getInt(1));
                }
            }
        }
    }
    ```


    In Spring Boot, ORM, and other third‑party frameworks built on JDBC, specify the same JDBC connection string; credentials from environment variables are picked up by the driver just like in the example above (for example, via `spring.datasource.url`).

  {% endlist %}

- JavaScript

  ```typescript
    import { Driver, getCredentialsFromEnv } from 'ydb-sdk';

    export async function connect(endpoint: string, database: string) {
        const authService = getCredentialsFromEnv();
        const driver = new Driver({endpoint, database, authService});
        const timeout = 10000;
        if (!await driver.ready(timeout)) {
            console.log(`Driver has not become ready in ${timeout}ms!`);
            process.exit(1);
        }
        console.log('Driver connected')
        return driver
    }
  ```

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-env](../../_includes/python/auth-env.md) %}

  - Native SDK (Asyncio)

    {% include [auth-env](../../_includes/python/async/auth-env.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa
    import ydb

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": ydb.credentials_from_env_variables()
        }
    )
    with engine.connect() as connection:
        result = connection.execute(sa.text("SELECT 1"))
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  ```rust
  use ydb::{ClientBuilder, FromEnvCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
      .with_credentials(FromEnvCredentials::new()?)
      .client()?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\EnvironCredentials;

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

      'credentials' => new EnvironCredentials()
  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
