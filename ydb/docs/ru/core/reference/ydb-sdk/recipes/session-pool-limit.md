---
title: "Инструкция по установке размера пула сессий в {{ ydb-short-name }}"
description: "Из статьи вы узнаете, как установить лимит на пул сессий в разных {{ ydb-short-name }} SDK."
---

# Установить размер пула сессий

{% include [work in progress message](_includes/addition.md) %}

Размер пула сессий на клиенте влияет на потребление ресурсов (память, процессор) на серверной стороне {{ ydb-short-name }}. Простая математика: если `1000` клиентов одной базы данных имеют по `1000` сессий, то на серверной стороне создается `1000000` акторов (воркеров, исполнителей сессий). Если не лимитировать количество сессий на клиенте, то можно получить "задумчивый" кластер в полу-аварийном состоянии.

По умолчанию в {{ ydb-short-name }} SDK установлен лимит в `50` сессий в случае использования нативных драйверов. При использовании внешних библиотек, например Go database/sql, лимит не установлен.

Хорошая рекомендация — устанавливать лимит на количество сессий на клиенте в минимальное-необходимое для штатной работы клиентского приложения. Следует иметь в виду, что сессия однопоточная что на серверной стороне, что на клиентской. Соответственно, если для расчетной нагрузки приложению необходимо выполнять  `1000` одновременных запросов (`inflight`) в {{ ydb-short-name }} — значит следует установить лимит в `1000` сессий.

Тут надо отличать расчетный `RPS` (requests per second, запросов в секунду) и `inflight`. В первом случае это общее количество выполненных запросов к {{ ydb-short-name }} за `1` секунду. Например, для `RPS`=`10000` и средним `latency` (задержка исполнения запроса) в `100`мс достаточно установить лимит в `1000` сессий. То есть каждая сессия за расчетную секунду выполнит в среднем `10` последовательных запросов.

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
  this.tableClient = TableClient.newClient(transport)
          // 10 - minimum number of active sessions to keep in the pool during the cleanup
          // 500 - maximum number of sessions in the pool
          .sessionPoolSize(10, 500)
          .build();
  ```

{% endlist %}
