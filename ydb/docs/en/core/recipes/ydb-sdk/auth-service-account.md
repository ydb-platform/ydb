# Authentication using a service account file

<!-- markdownlint-disable blanks-around-fences -->

Below are examples of authentication with a service account file in different {{ ydb-short-name }} SDKs.

{% list tabs %}

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
    public void work(String connectionString, String saKeyPath) {
        AuthProvider authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyPath);

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
    public void work() throws SQLException {
        Properties props = new Properties();
        props.setProperty("saKeyFile", "~/keys/sa_key.json");
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props)) {
            doWork(connection);
        }

        // You can also set saKeyFile in the JDBC URL
        try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local?saKeyFile=~/keys/sa_key.json")) {
            doWork(connection);
        }
    }
    ```

    In Spring Boot, ORMs, and other JDBC wrappers, use the same JDBC URL and `saKeyFile` (in the URL or in `DataSource` properties) as above.

  {% endlist %}

- JavaScript

  Loading service account data from a file:

  {% include [auth-sa-file](../../_includes/nodejs/auth-sa-file.md) %}

  Loading service account data from a third-party source (for example, a secrets store):

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

- C# (.NET)

  ```C#
  using Ydb.Sdk;
  using Ydb.Sdk.Yc;

  const string endpoint = "grpc://localhost:2136";
  const string database = "/local";

  var saProvider = new ServiceAccountProvider(
      saFilePath: "path/to/sa_file.json" // Path to file with service account JSON info);
  );
  await saProvider.Initialize();

  var config = new DriverConfig(
      endpoint: endpoint,
      database: database,
      credentials: saProvider
  );

  await using var driver = await Driver.CreateInitialized(config);
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
