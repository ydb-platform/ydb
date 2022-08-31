# Аутентификация при помощи логина и пароля

{% include [work in progress message](../_includes/addition.md) %}

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
      defer func() {
          _ = db.Close(ctx)
      }()
  }
  ```

{% endlist %}
