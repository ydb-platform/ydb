# Authentication using a service account file

<!-- markdownlint-disable blanks-around-fences -->

Authentication with a service account file uses a JSON key to create a JWT and obtain an IAM token. This method is suitable for server-side applications outside the cloud, CI/CD, and local development with access to {{ ydb-short-name }} in Yandex Cloud. Typical steps: save the service account key to a file, pass its path to the authentication provider, open a transport, and execute a request. Details are in the [Authentication](../../reference/ydb-sdk/auth.md) section; basic connection is in the [driver initialization](./init.md) recipe. Other methods: [token](./auth-access-token.md), [anonymous](./auth-anonymous.md), [environment variables](./auth-env.md), [metadata](./auth-metadata.md), [login and password](./auth-static.md).

Below are authentication code examples using a service account file in different {{ ydb-short-name }} SDKs.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/iam/iam.h>

    NYdb::TDriver CreateDriverWithServiceAccountKeyFile(
        const std::string& connectionString,
        const std::string& saKeyFilePath,
        const std::string& internalCA)
    {
        auto config = NYdb::TDriverConfig(connectionString)
            .UseSecureConnection(internalCA)
            .SetCredentialsProviderFactory(NYdb::CreateIamJwtFileCredentialsProviderFactory({
                .JwtFilename = saKeyFilePath,
            }));

        return NYdb::TDriver(config);
    }
    ```

  - userver

    {% cut "secdist" %}

    `<PEM>` – Yandex Cloud certificates.


    ```json
    {
      "ydb_settings": {
        "db": {
          "iam_jwt_params": {
            "id": "...",
            "service_account_id": "...",
            "private_key": "..."
          },
          "secure_connection_cert": "<PEM>"
        }
      }
    }
    ```

    {% endcut %}

    Initialization code `ydb::YdbComponent`, obtaining `ydb::TableClient` and starting `components::MinimalServerComponentList` — as in the example from [init.md](./init.md).

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

  - database/sql

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

    public class ServiceAccountAuthExample {
        public static void main(String[] args) throws Exception {
            // Connection string from environment variable or local {{ ydb-short-name }} by default
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            // Path to JSON key of the service account
            String saKeyFile = System.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            if (saKeyFile == null || saKeyFile.isEmpty()) {
                throw new IllegalStateException(
                        "Задайте переменную окружения YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            }

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
                    .withAuthProvider(CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile))
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
    import java.sql.Properties;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    public class ServiceAccountAuthJdbcExample {
        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            String saKeyFile = System.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            if (saKeyFile == null || saKeyFile.isEmpty()) {
                throw new IllegalStateException(
                        "Задайте переменную окружения YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            }

            Properties props = new Properties();
            props.setProperty("saKeyFile", saKeyFile);

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


    The `saKeyFile` option can also be specified directly in the JDBC URL: `jdbc:ydb:grpc://localhost:2136/local?saKeyFile=~/keys/sa_key.json`.

    In Spring Boot, ORM, and other third‑party frameworks built on JDBC, specify the same JDBC connection string and the `saKeyFile` parameter (in the URL or in the `DataSource` properties), as in the example above.

  {% endlist %}

- JavaScript

  Loading service account data from a file:

  {% include [auth-sa-file](../../_includes/nodejs/auth-sa-file.md) %}

  Loading service account data from an external source (for example, from a secret store):

  {% include [auth-sa-data](../../_includes/nodejs/auth-sa-data.md) %}

- Python

  {% list tabs %}

  - Native SDK

    {% include [auth-sa-data](../../_includes/python/auth-service-account.md) %}

  - Native SDK (Asyncio)

    {% include [auth-sa-data](../../_includes/python/async/auth-service-account.md) %}

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa
    import ydb.iam

    engine = sa.create_engine(
        "yql+ydb://localhost:2136/local",
        connect_args={
            "credentials": ydb.iam.ServiceAccountCredentials.from_file(
                os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]
            )
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
      "Host=ydb.serverless.yandexcloud.net;Port=2135;Database=/ru-central1/<folder-id>/<database-id>;ServiceAccountKeyFilePath=path/to/sa_file.json");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```


  For Entity Framework and linq2db, use the same connectionString.

- Rust

  ```rust
  use ydb::{ClientBuilder, ServiceAccountCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
      .with_credentials(ServiceAccountCredentials::from_env()?)
      .client()?;
  ```

- PHP

  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\JwtWithJsonAuthentication;

  $config = [
      'database'    => '/ru-central1/b1glxxxxxxxxxxxxxxxx/etn0xxxxxxxxxxxxxxxx',
      'endpoint'    => 'ydb.serverless.yandexcloud.net:2135',
      'discovery'   => false,
      'iam_config'  => [
          'temp_dir'       => './tmp', // Temp directory
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)ы
      ],

      'credentials' => new JwtWithJsonAuthentication('./jwtjson.json')
  ];

  $ydb = new Ydb($config);
  ```


  or


  ```php
  <?php

  use YdbPlatform\Ydb\Ydb;
  use YdbPlatform\Ydb\Auth\JwtWithPrivateKeyAuthentication;

  $config = [
      'database'    => '/ru-central1/b1glxxxxxxxxxxxxxxxx/etn0xxxxxxxxxxxxxxxx',
      'endpoint'    => 'ydb.serverless.yandexcloud.net:2135',
      'discovery'   => false,
      'iam_config'  => [
          'temp_dir'           => './tmp', // Temp directory
          // 'root_cert_file' => './CA.pem', // Root CA file (uncomment for dedicated server)

      ],

      'credentials' => new JwtWithPrivateKeyAuthentication(
          "ajexxxxxxxxx","ajeyyyyyyyyy",'./private.key')

  ];

  $ydb = new Ydb($config);
  ```

{% endlist %}
