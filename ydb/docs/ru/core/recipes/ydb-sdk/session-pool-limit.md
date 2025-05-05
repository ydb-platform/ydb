# Установить размер пула сессий

{{ ydb-short-name }} создаёт [актор](../../concepts/glossary.md#actor) для каждой сессии. В результате, размер пула сессий на клиенте влияет на потребление ресурсов (память, процессор) на серверной стороне {{ ydb-short-name }}.

Например, если 1000 клиентов одной базы данных открывают по 1000 сессий, то на серверной стороне создаётся 1000000 акторов. Такое количество акторов потребляет значительные объёмы памяти и ресурсов процессора. При отсутствии ограничения на число сессий на клиенте это может привести к медленной работе кластера и его полуаварийному состоянию.

По умолчанию в {{ ydb-short-name }} SDK при использовании нативных драйверов установлен лимит в 50 сессий. При использовании сторонних библиотек, например, Go `database/sql`, лимит не задан.

Рекомендуется устанавливать лимит на количество сессий на клиенте в минимально необходимый для штатной работы клиентского приложения. Следует учитывать, что сессия однопоточная как на серверной, так и на клиентской стороне. Соответственно, если для расчётной нагрузки приложению требуется выполнять 1000 одновременных запросов (inflight) в {{ ydb-short-name }}, то лимит следует установить на уровне 1000 сессий.

Важно различать расчётный RPS (requests per second, запросов в секунду) и inflight. В первом случае речь идёт об общем количестве запросов, выполняемых к {{ ydb-short-name }} за 1 секунду. Например, при RPS = 10000 и средней задержке исполнения запроса (latency) в 100&nbsp;мс достаточно установить лимит в 1000 сессий. Это означает, что каждая сессия за расчётную секунду выполнит в среднем 10 последовательных запросов.

Ниже приведены примеры кода установки лимита на пул сессий в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

  ```golang
  package main

  import (
    "context"

    "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      ydb.WithSessionPoolSizeLimit(500),
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)
    ...
  }
  ```

- Go (database/sql)

  Библиотека `database/sql` имеет свой пул соединений. Каждое соединение в `database/sql` соответствует конкретной сессии {{ ydb-short-name }}. Управлением пулом соединений в `database/sql` осуществляется с помощью функций `sql.DB.SetMaxOpenConns` и `sql.DB.SetMaxIdleConns`. Подробнее об этом написано в [документации](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns) `database/sql`.

  Пример кода, использующего размер пула соединений `database/sql`:

  ```golang
  package main

  import (
    "context"
    "database/sql"

    _ "github.com/ydb-platform/ydb-go-sdk/v3"
  )

  func main() {
    db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
    if err != nil {
      panic(err)
    }
    defer db.Close()
    db.SetMaxOpenConns(100)
    db.SetMaxIdleConns(100)
    db.SetConnMaxIdleTime(time.Second) // workaround for background keep-aliving of YDB sessions
    ...
  }
  ```

- Java

  ```java
  this.queryClient = QueryClient.newClient(transport)
          // 10 - minimum number of active sessions to keep in the pool during the cleanup
          // 500 - maximum number of sessions in the pool
          .sessionPoolMinSize(10)
          .sessionPoolMaxSize(500)
          .build();
  ```

- JDBC Driver

  При работе с JDBC, как правило, используются внешние пулы соединений, такие как [HikariCP](https://github.com/brettwooldridge/HikariCP) или [C3p0](https://github.com/swaldman/c3p0). В режиме работы по умолчанию {{ ydb-short-name }} JDBC драйвер определяет количество соединений, открытых внешним пулом, и самостоятельно подстраивает размер пула сессий. Поэтому для настройки пула сессий достаточно корректно настроить `HikariCP` или `C3p0`.

  Пример настройки пула HikariCP в конфигурационном файле Spring:

  ```properties
    spring.datasource.url=jdbc:ydb:grpc://localhost:2136/local
    spring.datasource.driver-class-name=tech.ydb.jdbc.YdbDriver
    spring.datasource.hikari.maximum-pool-size=100 # maximum size of JDBC connections
  ```


{% endlist %}
