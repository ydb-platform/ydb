# Username and password based authentication

{% include [work in progress message](../_includes/addition.md) %}

Below are examples of the code for authentication based on a username and token in different {{ ydb-short-name }} SDKs.

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
