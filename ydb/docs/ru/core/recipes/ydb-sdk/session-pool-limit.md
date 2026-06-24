# Установить размер пула сессий

{{ ydb-short-name }} создаёт [актор](../../concepts/glossary.md#actor) для каждой сессии. В результате, размер пула сессий на клиенте влияет на потребление ресурсов (память, процессор) на серверной стороне {{ ydb-short-name }}.

Например, если 1000 клиентов одной базы данных открывают по 1000 сессий, то на серверной стороне создаётся 1000000 акторов. Такое количество акторов потребляет значительные объёмы памяти и ресурсов процессора. При отсутствии ограничения на число сессий на клиенте это может привести к медленной работе кластера и его полуаварийному состоянию.

По умолчанию в {{ ydb-short-name }} SDK при использовании нативных драйверов установлен лимит в 50 сессий. При использовании сторонних библиотек, например, Go `database/sql`, лимит не задан.

Рекомендуется устанавливать лимит на количество сессий на клиенте в минимально необходимый для штатной работы клиентского приложения. Следует учитывать, что сессия однопоточная как на серверной, так и на клиентской стороне. Соответственно, если для расчётной нагрузки приложению требуется выполнять 1000 одновременных запросов (inflight) в {{ ydb-short-name }}, то лимит следует установить на уровне 1000 сессий.

Важно различать расчётный RPS (requests per second, запросов в секунду) и inflight. В первом случае речь идёт об общем количестве запросов, выполняемых к {{ ydb-short-name }} за 1 секунду. Например, при RPS = 10000 и средней задержке исполнения запроса (latency) в 100&nbsp;мс достаточно установить лимит в 1000 сессий. Это означает, что каждая сессия за расчётную секунду выполнит в среднем 10 последовательных запросов.

Ниже приведены примеры кода установки лимита на пул сессий в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    - `MaxActiveSessions` — максимальный размер пула (по умолчанию 50).
    - `MinPoolSize` — минимальное число сессий (по умолчанию 10). SDK перестанет закрывать сессии по таймауту по достижении лимита, поэтому число не гарантированное.

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>
    #include <ydb-cpp-sdk/client/query/client.h>

    NYdb::NQuery::TQueryClient CreateQueryClient(const NYdb::TDriver& driver) {
        NYdb::NQuery::TClientSettings settings;
        settings.SessionPoolSettings(
            NYdb::NQuery::TSessionPoolSettings()
                .MaxActiveSessions(500)
                .MinPoolSize(10));
        return NYdb::NQuery::TQueryClient(driver, settings);
    }
    ```

  - userver

    {% cut "static config" %}

    ```yaml
    ydb:
        databases:
            db:
                endpoint: grpc://localhost:2136
                database: /local
                max_pool_size: 500
                min_pool_size: 10
    ```

    {% endcut %}

    Код инициализации `ydb::YdbComponent`, получения `ydb::TableClient` и запуска `components::MinimalServerComponentList` — как в примере из [init.md](./init.md).

  {% endlist %}

- Go

  {% list tabs %}

  - Native SDK

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

  - database/sql

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

  {% endlist %}

- Java

  {% list tabs %}

  - Native SDK

    Размер пула задаётся при создании `TableClient` или `QueryClient`. Каждая сессия в пуле — отдельный [актор](../../concepts/glossary.md#actor) на сервере, поэтому лимит следует выбирать по расчётному inflight, а не только по RPS (см. вводный раздел выше).

    ```java
    import tech.ydb.common.transaction.TxMode;
    import tech.ydb.core.grpc.GrpcTransport;
    import tech.ydb.query.QueryClient;
    import tech.ydb.query.result.ResultSetReader;
    import tech.ydb.query.tools.QueryReader;
    import tech.ydb.query.tools.SessionRetryContext;
    import tech.ydb.table.query.Params;

    public class SessionPoolLimitExample {

        public static void main(String[] args) {
            String connectionString = System.getenv().getOrDefault(
                    "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

            try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
                 QueryClient queryClient = QueryClient.newClient(transport)
                         // 10 — минимальное число активных сессий, удерживаемых в пуле при очистке
                         // 500 — максимальный размер пула сессий
                         .sessionPoolMinSize(10)
                         .sessionPoolMaxSize(500)
                         .build()) {

                SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();

                QueryReader reader = retryCtx.supplyResult(session -> QueryReader.readFrom(
                        session.createQuery("SELECT 1 AS value", TxMode.NONE, Params.empty())
                )).join().getValue();

                ResultSetReader rs = reader.getResultSet(0);
                if (rs.next()) {
                    System.out.println("Пул настроен, SELECT 1 = " + rs.getColumn("value").getInt32());
                }
            }
        }
    }
    ```

  - JDBC

    При работе с JDBC, как правило, используются внешние пулы соединений, такие как [HikariCP](https://github.com/brettwooldridge/HikariCP). {{ ydb-short-name }} JDBC-драйвер подстраивает внутренний пул сессий под число открытых JDBC-соединений, поэтому лимит задаётся на стороне внешнего пула. Свойства подключения — в [документации JDBC-драйвера](../../reference/languages-and-apis/jdbc-driver/properties.md).

    ```java
    import java.sql.Connection;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;

    import com.zaxxer.hikari.HikariConfig;
    import com.zaxxer.hikari.HikariDataSource;

    public class JdbcSessionPoolLimitExample {

        public static void main(String[] args) throws SQLException {
            String jdbcUrl = System.getenv().getOrDefault(
                    "YDB_JDBC_URL", "jdbc:ydb:grpc://localhost:2136/local");

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setDriverClassName("tech.ydb.jdbc.YdbDriver");
            config.setMaximumPoolSize(100); // максимум JDBC-соединений (= сессий YDB)

            try (HikariDataSource dataSource = new HikariDataSource(config);
                 Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT 1 AS value")) {

                rs.next();
                System.out.println("HikariCP pool size = 100, SELECT 1 = " + rs.getInt("value"));
            }
        }
    }
    ```

    В Spring Boot те же параметры задаются через `spring.datasource.hikari.maximum-pool-size` (см. пример в вводном разделе).

  {% endlist %}

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import os
    import ydb

    with ydb.Driver(
        connection_string=os.environ["YDB_CONNECTION_STRING"],
        credentials=ydb.credentials_from_env_variables(),
    ) as driver:
        driver.wait(timeout=5)
        with ydb.QuerySessionPool(driver, size=500) as pool:
            # ...
    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb
    import asyncio

    async def ydb_init():
        async with ydb.aio.Driver(
            connection_string=os.environ["YDB_CONNECTION_STRING"],
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            await driver.wait()
            async with ydb.aio.QuerySessionPool(driver, size=500) as pool:
                # ...

    asyncio.run(ydb_init())
    ```

  - SQLAlchemy

    Установка размера пула на данный момент не поддержана.

  {% endlist %}

- C#

  В {{ ydb-short-name }} C# SDK параметры пула сессий задаются через строку подключения:

  ```C#
  using Ydb.Sdk.Ado;

  await using var dataSource = new YdbDataSource(
      "Host=localhost;Port=2136;Database=/local;MaxPoolSize=500;MinPoolSize=10;SessionIdleTimeout=60");
  ```

  * `MaxPoolSize` — максимальный размер пула сессий (по умолчанию 100)
  * `MinPoolSize` — минимальное число сессий, удерживаемых в пуле (по умолчанию 0)
  * `SessionIdleTimeout` — время простоя сессии в секундах до её закрытия (по умолчанию 300)

  Для Entity Framework и linq2db используйте тот же connectionString.

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
