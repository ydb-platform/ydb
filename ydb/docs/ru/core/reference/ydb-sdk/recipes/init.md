# Инициализация драйвера

{% include [work in progress message](_includes/addition.md) %}

Для подключения к {{ ydb-short-name }} требуется указать обязательные параметры (подробнее читайте в разделе [Подключение к серверу {{ ydb-short-name }}](../../../concepts/connect.md)) и дополнительные, которые определяют поведение драйвера при работе.

Ниже приведены примеры кода подлкючения к {{ ydb-short-name }} (создания драйвера) в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

  ```golang
  package main

  import (
    "context"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
    if err != nil {
        log.Fatal(err)
    }
    ...
  }
  ```

- Go (database/sql)

  {% cut "С помощью коннектора (рекомендуемый способ)" %}
    ```golang
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
  {% endcut %}

  {% cut "С помощью строки подключения" %}

    Регистрация драйвера `database/sql` реализуется в момент импорта пакета конкретного драйвера через символ подчеркивавния:
    ```golang
    package main

    import (
      "context"
      "database/sql"
      "os"

      _ "github.com/ydb-platform/ydb-go-sdk/v3"
    )

    func main() {
      db, err := sql.Open("ydb", "grpc://localhost:2136/local")
      if err != nil {
          log.Fatal(err)
      }
      ...
    }
    ```
  {% endcut %}

- Java


  {% include [work in progress message](_includes/addition.md) %}

{% endlist %}
