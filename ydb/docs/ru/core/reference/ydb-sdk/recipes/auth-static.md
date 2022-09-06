# Аутентификация при помощи логина и пароля

{% include [work in progress message](_includes/addition.md) %}

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

- Go (native)

  Передать логин и пароль можно в составе строки подключения. Например, так:
  ```shell
  "grpcs://login:password@localohost:2135/local"
  ```

  Также можно передать логин и пароль явно через опцию `ydb.WithStaticCredentials`:
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
        ydb.WithStaticCredentials("user", "password"),
    )
    if err != nil {
        panic(err)
    }
    defer db.Close(ctx)
    ...
  }
  ```

- Go (database/sql)

  Передать логин и пароль можно в составе строки подключения. Например, так:
  ```go
  package main

  import (
    "context"

    _ "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    db, err := sql.Open("ydb", "grpcs://login:password@localohost:2135/local")
    if err != nil {
        panic(err)
    }
    defer db.Close()
    ...
  }
  ```

  Также можно передать логин и пароль явно при инициализации драйвера через коннектор с помощью специальной опции `ydb.WithStaticCredentials`:
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
    nativeDriver, err := ydb.Open(ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydb.WithStaticCredentials("user", "password"),
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
