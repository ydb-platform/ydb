# Аутентификация при помощи файла сервисного аккаунта

<!-- markdownlint-disable blanks-around-fences -->

Ниже приведены примеры кода аутентификации при помощи файла сервисного аккаунта в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

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

- Go (database/sql)

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

- Java

  ```java
  public void work(String connectionString, String saKeyPath) {
      AuthProvider authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyPath);

      GrpcTransport transport = GrpcTransport.forConnectionString(connectionString)
              .withAuthProvider(authProvider)
              .build());

      QueryClient queryClient = QueryClient.newClient(transport).build();

      doWork(queryClient);

      queryClient.close();
      transport.close();
  }
  ```

- JDBC

  ```java
  public void work() {
      Properties props = new Properties();
      props.setProperty("saKeyFile", "~/keys/sa_key.json");
      try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local", props)) {
        doWork(connection);
      }

      // Опцию saKeyFile также можно указать прямо в JDBC URL
      try (Connection connection = DriverManager.getConnection("jdbc:ydb:grpc://localhost:2136/local?saKeyFile=~/keys/sa_key.json")) {
        doWork(connection);
      }
  }
  ```

- Node.js

  Загрузка данных сервисного аккаунта из файла:

  {% include [auth-sa-file](../../_includes/nodejs/auth-sa-file.md) %}

  Загрузка данных сервисного аккаунта из стороннего источника (например, из хранилища секретов):

  {% include [auth-sa-data](../../_includes/nodejs/auth-sa-data.md) %}

- Python

  {% include [auth-sa-data](../../_includes/python/auth-service-account.md) %}

- Python (asyncio)

  {% include [auth-sa-data](../../_includes/python/async/auth-service-account.md) %}

- C#

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource(
      "Host=ydb.serverless.yandexcloud.net;Port=2135;Database=/ru-central1/<folder-id>/<database-id>;ServiceAccountKeyFilePath=path/to/sa_file.json");
  await using var connection = await dataSource.OpenConnectionAsync();
  ```

<<<<<<< HEAD
=======
  Для Entity Framework и linq2db используйте тот же connectionString.

- Rust

  ```rust
  use ydb::{ClientBuilder, ServiceAccountCredentials, YdbResult};

  let client = ClientBuilder::new_from_connection_string(std::env::var("YDB_CONNECTION_STRING")?)?
      .with_credentials(ServiceAccountCredentials::from_env()?)
      .client()?;
  ```

>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))
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

  или

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
