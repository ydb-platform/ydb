# Предпочитать ближайший дата-центр

Ниже приведены примеры кода установки опции алгоритма балансировки "предпочитать ближайший дата-центр" в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- Go (native)

  ```go
  package main

  import (
    "context"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    db, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      ydb.WithBalancer(
        balancers.PreferLocalDC(
          balancers.RandomChoice(),
        ),
      ),
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)
    ...
  }
  ```

- Go (database/sql)

  Клиентская балансировка в `database/sql` драйвере для {{ ydb-short-name }} осуществляется только в момент установления нового соединения (в терминах `database/sql`), которое представляет собой сессию {{ ydb-short-name }} на конкретной ноде. После того, как сессия создана, все запросы на этой сессии направляются на ту ноду, на которой была создана сессия. Балансировка запросов на одной и той же сессии {{ ydb-short-name }} между разными нодами {{ ydb-short-name }} не происходит.

  Пример кода установки алгоритма балансировки "предпочитать ближайший дата-центр":

  ```go
  package main

  import (
    "context"
    "database/sql"
    "os"

    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/balancers"
  )

  func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    nativeDriver, err := ydb.Open(ctx,
      os.Getenv("YDB_CONNECTION_STRING"),
      ydb.WithBalancer(
        balancers.PreferLocalDC(
          balancers.RandomChoice(),
        ),
      ),
    )
<<<<<<< HEAD
    if err != nil {
      panic(err)
=======

    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=5)
        # ...
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb
    import asyncio

    async def ydb_init():
        driver_config = ydb.DriverConfig(
            endpoint=os.environ["YDB_ENDPOINT"],
            database=os.environ["YDB_DATABASE"],
            credentials=ydb.credentials_from_env_variables(),
            use_all_nodes=False,  # предпочитать ближайший дата-центр
        )
        async with ydb.aio.Driver(driver_config) as driver:
            await driver.wait()
            # ...

    asyncio.run(ydb_init())
    ```

  - SQLAlchemy

    ```python
    import os
    import sqlalchemy as sa

    engine = sa.create_engine(
        os.environ["YDB_SQLALCHEMY_URL"],
        connect_args={
            "driver_config_kwargs": {
                "use_all_nodes": False,  # предпочитать ближайший дата-центр
            }
        },
    )
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% list tabs %}

  - Native SDK

    ```java
    import tech.ydb.core.grpc.BalancingSettings;
    import tech.ydb.core.grpc.GrpcTransport;

    try (GrpcTransport transport = GrpcTransport.forConnectionString("grpc://localhost:2136/local")
            .withBalancingSettings(BalancingSettings.detectLocalDs())
            .build()) {
        // ...
>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))
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
