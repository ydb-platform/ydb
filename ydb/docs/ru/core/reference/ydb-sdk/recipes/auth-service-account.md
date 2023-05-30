# Аутентификация при помощи файла сервисного аккаунта

{% include [work in progress message](_includes/addition.md) %}

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

      TableClient tableClient = TableClient.newClient(transport).build();

      doWork(tableClient);

      tableClient.close();
      transport.close();
  }
  ```

- Node.js

  Загрузка данных сервисного аккаунта из файла:

  {% include [auth-sa-file](../../../../_includes/nodejs/auth-sa-file.md) %}

  Загрузка данных сервисного аккаунта из стороннего источника (например, из хранилища секретов):

  {% include [auth-sa-data](../../../../_includes/nodejs/auth-sa-data.md) %}

- Python

  {% include [auth-sa-data](../../../../_includes/python/auth-service-account.md) %}

- Python (asyncio)

  {% include [auth-sa-data](../../../../_includes/python/async/auth-service-account.md) %}

{% endlist %}
