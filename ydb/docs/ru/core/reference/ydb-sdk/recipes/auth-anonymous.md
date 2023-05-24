# Анонимная аутентификация

{% include [work in progress message](_includes/addition.md) %}

Ниже приведены примеры кода анонимной аутентификации в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

  Анонимная аутентификация является аутентификацией по умолчанию.
  Явным образом анонимную аутентификацию можно включить так:
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

- Go (database/sql)

  Анонимная аутентификация является аутентификацией по умолчанию.
  Явным образом анонимную аутентификацию можно включить так:
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

- Java

  ```java
  public void work(String connectionString) {
      AuthProvider authProvider = NopAuthProvider.INSTANCE;

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

  {% include [auth-anonymous](../../../../_includes/nodejs/auth-anonymous.md) %}

- Python

  {% include [auth-anonymous](../../../../_includes/python/auth-anonymous.md) %}

- Python (asyncio)

  {% include [auth-anonymous](../../../../_includes/python/async/auth-anonymous.md) %}

- {% endlist %}
