# Равномерный случайный выбор

{{ ydb-short-name }} SDK использует алгоритм `random_choice` (равномерную случайную балансировку) по умолчанию, кроме С++ SDK, который использует алгоритм ["предпочитать ближайший дата-центр"](./balancing-prefer-local.md) по умолчанию.

Ниже приведены примеры кода принудительной установки алгоритма балансировки "равномерный случайный выбор" в разных {{ ydb-short-name }} SDK.

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
        balancers.RandomChoice(),
      ),
    )
    if err != nil {
      panic(err)
    }
    defer db.Close(ctx)
    // ...
  }
  ```

- Go (database/sql)

  Клиентская балансировка в `database/sql` драйвере для {{ ydb-short-name }} осуществляется только в момент установления нового соединения (в терминах `database/sql`), которое представляет собой сессию {{ ydb-short-name }} на конкретной ноде. После того, как сессия создана, все запросы на этой сессии направляются на ту ноду, на которой была создана сессия. Балансировка запросов на одной и той же сессии {{ ydb-short-name }} между разными нодами {{ ydb-short-name }} не происходит.

  Пример кода установки алгоритма балансировки "равномерный случайный выбор":

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
        balancers.RandomChoice(),
      ),
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
    // ...
  }
  ```

- C++

  ```cpp
  #include <ydb-cpp-sdk/client/driver/driver.h>

  int main() {
    auto connectionString = std::string(std::getenv("YDB_CONNECTION_STRING"));

    auto driverConfig = NYdb::TDriverConfig(connectionString)
      .SetBalancingPolicy(NYdb::TBalancingPolicy::UseAllNodes());

    NYdb::TDriver driver(driverConfig);
    // ...
    driver.Stop(true);
    return 0;
  }
  ```

- Python

  {% cut "sqlalchemy" %}

  ```python
  import os
  import sqlalchemy as sa

  engine = sa.create_engine(
      os.environ["YDB_SQLALCHEMY_URL"],
      connect_args={
          "driver_config_kwargs": {
              "use_all_nodes": True,  # равномерный случайный выбор
          }
      },
  )
  ```

  {% endcut %}

  {% cut "asyncio" %}

  ```python
  import os
  import ydb
  import asyncio

  async def ydb_init():
      driver_config = ydb.DriverConfig(
          endpoint=os.environ["YDB_ENDPOINT"],
          database=os.environ["YDB_DATABASE"],
          credentials=ydb.credentials_from_env_variables(),
          use_all_nodes=True,  # равномерный случайный выбор
      )
      async with ydb.aio.Driver(driver_config) as driver:
          await driver.wait()
          # ...

  asyncio.run(ydb_init())
  ```

  {% endcut %}

  ```python
  import os
  import ydb

  driver_config = ydb.DriverConfig(
      endpoint=os.environ["YDB_ENDPOINT"],
      database=os.environ["YDB_DATABASE"],
      credentials=ydb.credentials_from_env_variables(),
      use_all_nodes=True,  # равномерный случайный выбор
  )

  with ydb.Driver(driver_config) as driver:
      driver.wait(timeout=5)
      # ...
  ```

{% endlist %}
