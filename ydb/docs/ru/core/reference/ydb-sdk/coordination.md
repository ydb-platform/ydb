# Работа с узлами координации

Данная статья описывает как использовать {{ ydb-short-name }} SDK для координации работы нескольких экземпляров клиентского приложения посредством использования [узлов координации](../../concepts/datamodel/coordination-node.md) и находящихся в них семафоров.

## Создание узла координации

Узлы координации создаются в базах данных {{ ydb-short-name }} в том же пространстве имён, что и другие объекты схемы, такие как [таблицы](../../concepts/datamodel/table.md) и [топики](../../concepts/datamodel/topic.md).

{% list tabs %}

- C++

    ```cpp
    TClient client(driver);
    auto status = client
        .CreateNode("/path/to/mynode")
        .ExtractValueSync();
    Y_ABORT_UNLESS(status.IsSuccess());
    ```

   При создании можно опционально указать `TNodeSettings` со следующими настройками:

   - `ReadConsistencyMode` - по умолчанию `RELAXED`, что допускает чтение не самого свежего значения в случае смены лидера. Опционально можно включить `STRICT` режим чтения, при котором все чтения проходят через алгоритм консенсуса и гарантируют возврат самого свежего значения, но становятся существенно дороже.
   - `AttachConsistencyMode` - по умолчанию `STRICT`, что означает обязательное использование алгоритма консенсуса при восстановлении сессии. Опционально можно включить `RELAXED` режим восстановления сессии в случае сбоев, который отключает это требование. Расслабленный режим может потребоваться при очень большом количестве клиентов, позволяя восстанавливать сессию без прохождения через консенсус, что не влияет на общую корректность, но может усугублять чтение не самого свежего значения во время смены лидера, а также устаревание сессий в случае проблем.
   - `SelfCheckPeriod` (по умолчанию 1 секунда) - периодичность с которой сервис производит проверки собственной живости. Не рекомендуется менять за исключением особых случаев.

     - Чем больше указанное значение, тем меньше нагрузка на сервер, но тем дольше возможная задержка между сменой лидера и тем, насколько оперативно об этом узнает сам сервис.
     - Чем меньше указанное значение, тем больше нагрузка на сервер и большая оперативность в детектировании проблем, но возможна генерация false positive когда сервис ошибочно детектирует проблемы.

   - `SessionGracePeriod` (по умолчанию 10 секунд) - период, в течение которого новый лидер не закрывает открытые сессии, продлевая их.

     - Чем меньше значение, тем меньше окно, когда сессии от несуществующих клиентов, которые не успели сообщить о пропаже при смене лидера, будут удерживать семафоры и мешать другим клиентам.
     - Чем меньше значение, тем выше вероятность ложных срабатываний, когда живой лидер может завершить работу для перестраховки, так как не будет уверен, что этот период не закончился у нового лидера.
     - Должен быть строго больше, чем `SelfCheckPeriod`.

- Go

    ```go
    err := db.Coordination().CreateNode(ctx,
        "/path/to/mynode",
    )
    ```

- Java

  ```java
  CoordinationClient client = CoordinationClient.newClient(transport);
  ```

  Узел создаётся вызовом `createNode` с полным путём к узлу в базе. Префикс пути к базе можно взять из `client.getDatabase()`.

  При необходимости задайте конфигурацию узла через [NodeConfig](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/NodeConfig.java), используя цепочку `NodeConfig.create().with…`. Доступные параметры: периоды `SelfCheckPeriod` и `SessionGracePeriod`, режимы согласованности чтения и подключения сессии (`readConsistencyMode`, `attachConsistencyMode`), режим счётчиков ограничителя скорости (`rateLimiterCountersMode`). Значения по умолчанию совпадают с описанием для C++ (см. выше). Готовый `NodeConfig` передаётся в `CoordinationNodeSettings`.

  ```java
  import java.time.Duration;

  import tech.ydb.coordination.CoordinationClient;
  import tech.ydb.coordination.description.NodeConfig;
  import tech.ydb.coordination.settings.CoordinationNodeSettings;

  String nodePath = client.getDatabase() + "/path/to/mynode";

  NodeConfig config = NodeConfig.create()
      .withDurationsConfig(Duration.ofSeconds(1), Duration.ofSeconds(10))
      .withReadConsistencyMode(NodeConfig.ConsistencyMode.RELAXED)
      .withAttachConsistencyMode(NodeConfig.ConsistencyMode.STRICT);

  CoordinationNodeSettings settings = CoordinationNodeSettings.newBuilder()
      .withNodeConfig(config)
      .build();

  client.createNode(nodePath, settings).join().expectSuccess("create node failed");
  ```

  Дополнительно доступны `alterNode` (изменение конфигурации), `dropNode` (удаление узла) и `describeNode` (чтение текущей конфигурации).

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    client.create_node("/path/to/mynode")
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    await client.create_node("/path/to/mynode")
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  import { CoordinationClient } from "@ydbjs/coordination";

  let client = new CoordinationClient(driver);
  await client.createNode("/path/to/mynode", {});
  ```

- Rust

  Клиент координации возвращается из [`Client::coordination_client`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.coordination_client). Узел создаётся через [`CoordinationClient::create_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_node) с путём и [`NodeConfig`](https://docs.rs/ydb/latest/ydb/struct.NodeConfig.html) (через [`NodeConfigBuilder`](https://docs.rs/ydb/latest/ydb/struct.NodeConfigBuilder.html)). Также доступны [`alter_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.alter_node), [`drop_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.drop_node), [`describe_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.describe_node). Полный пример — [`mutex.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/mutex.rs).

  ```rust
  use ydb::NodeConfigBuilder;

  let mut coordination_client = client.coordination_client();

  coordination_client
      .create_node(
          "/path/to/mynode".into(),
          NodeConfigBuilder::default().build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Работа с сессиями {#session}

### Создание сессии {#create-session}

Для начала работы клиент должен установить сессию, в рамках которой он будет осуществлять все операции с узлом координации.

{% list tabs %}

- C++

    ```cpp
    TClient client(driver);
    const TSession& session = client
       .StartSession("/path/to/mynode")
       .ExtractValueSync()
       .ExtractResult();
    ```

   При установке сессии можно опционально передать структуру `TSessionSettings` со следующими настройками:

   - `Description` - текстовое описацие сессии, отображается во внутренних интерфейсах и может быть полезно при диагностике проблем.
   - `OnStateChanged` - вызывается на важных изменениях в процессе жизни сессии, передавая соответствующее состояние:

     - `ATTACHED` - сессия подключена и работает в нормальном режиме;
     - `DETACHED` - сессия временно потеряла связь с сервисом, но ещё может быть восстановлена;
     - `EXPIRED` - сессия потеряла связь с сервисом и не может быть восстановлена.

   - `OnStopped` - вызывается, когда сессия прекращает попытки восстановить связь с сервисом, что может быть полезно для установления нового соединения.
   - `Timeout` - максимальный таймаут, в течение которого сессия может быть восстановлена после потери связи с сервисом.

- Go

    ```go
    session, err := db.Coordination().CreateSession(ctx,
        "/path/to/mynode", // имя Coordination Node в базе
    )
    ```

- Java

  Сессия (см. [CoordinationSession](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/CoordinationSession.java)) создаётся через `createSession`; для установления двунаправленного gRPC-потока с узлом нужно вызвать `connect()` (асинхронно, возвращает `CompletableFuture<Status>`). Параметры повторных попыток и таймаут подключения задаются в [CoordinationSessionSettings](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/CoordinationSessionSettings.java) (`withConnectTimeout`, `withRetryPolicy`, `withExecutor`).

  ```java
  import tech.ydb.coordination.CoordinationSession;
  import tech.ydb.coordination.settings.CoordinationSessionSettings;

  CoordinationSession session = client.createSession(
      "/path/to/mynode",
      CoordinationSessionSettings.newBuilder().build()
  );

  session.connect().join().expectSuccess("connect failed");
  ```

  Обычный сценарий: после успешного `connect()` выполняете операции с семафорами под свою задачу (блокировка, лидерство и т.д.), затем закрываете сессию. Удобно использовать try-with-resources, чтобы по завершении работы вызвать `close()` и корректно остановить поток с узлом.

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # работа с сессией
        pass
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # работа с сессией
        pass
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  import { CoordinationClient } from "@ydbjs/coordination";

  let client = new CoordinationClient(driver);
  await using session = await client.createSession("/path/to/mynode", {}, signal);
  ```

- Rust

  Сессию создаёт [`CoordinationClient::create_session`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_session) с путём к узлу и [`SessionOptions`](https://docs.rs/ydb/latest/ydb/struct.SessionOptions.html) ([`SessionOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.SessionOptionsBuilder.html): таймаут, описание и т.д.). Поток с узлом поднимается внутри конструктора сессии; отдельного вызова `connect`, как в Java, нет.

  ```rust
  use ydb::SessionOptionsBuilder;

  let session = coordination_client
      .create_session(
          "/path/to/mynode".into(),
          SessionOptionsBuilder::default().build()?,
      )
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Контроль завершения сессии {#session-control}

Клиентскому приложению необходимо следить за состоянием сессии, так как оно может полагаться на состояние захваченных семафоров только пока сессия активна. Когда сессия завершается по инициативе клиента или сервера, клиент больше не может быть уверен, что другие клиенты в кластере не захватили его семафоры и не изменили их состояние.

{% list tabs %}

- C++

  В C++ SDK установленная сессия в фоне поддерживает и автоматически восстанавливает связь с кластером {{ ydb-short-name }}.

- Go

  В Go SDK для отслеживания таких ситуаций используется контекст сессии `session.Context()`, который завершается вместе с сессией. SDK самостоятельно обрабатывает ошибки транспортного уровня и восстанавливает соединение с сервисом, пытаясь восстановить сессию, если это возможно. Таким образом, клиенту достаточно следить только за контекстом сессии, чтобы своевременно отреагировать на её потерю.

- Python

  В Python SDK сессия автоматически восстанавливает связь с кластером {{ ydb-short-name }} при сбоях. Рекомендуется использовать контекстный менеджер (`with` или `async with`) для гарантированного закрытия сессии при выходе из блока. При работе с семафорами через контекстный менеджер (`with session.semaphore(name)` или `async with session.semaphore(name)`) семафор автоматически освобождается при выходе из блока, а сессия — при закрытии контекста.

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  В JS SDK для отслеживания таких ситуаций используется сигнал `session.signal`, который прерывается вместе с сессией. SDK самостоятельно обрабатывает ошибки транспортного уровня и восстанавливает соединение с сервисом, пытаясь восстановить сессию, если это возможно. Таким образом, клиенту достаточно следить за сигналом сессии, чтобы не совершать действий когда сессия была закрыта или просрочена.

  Также в JavaScript SDK есть метод для получения новой сессии при утрате старой, и этот способ является рекомендованным для длительного использования `for await (session of client.openSession()) { session.signal }`.

- Java

  Завершите сессию (`close()`), когда ваш сценарий отработал: так вы явно освободите соединение с узлом. Пока сессия не закрыта, SDK при сбоях сети сам повторяет подключение согласно `CoordinationSessionSettings`. Семафор держите только на время решения пользовательской задачи и отпускайте через `SemaphoreLease.release()`, когда ресурс больше не нужен.

- Rust

  У [`CoordinationSession`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html) вызовите [`alive`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.alive): вернётся [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) — при завершении сессии он отменяется (аналог отслеживания контекста в Go). При отпускании [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html) или при `Drop` сессии освобождение семафора уходит на сервер в фоне.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Работа с семафорами {#semaphore}

### Создание семафора {#create-semaphore}

При создании семафора можно указать его лимит. Лимит определяет максимальное значение, на которое его можно увеличить. Вызовы, пытающиеся увеличить значение семафора выше этого лимита, начнут ждать, пока их запросы на увеличение смогут быть выполнены, так чтобы значение семафора не превышало его лимит.

{% list tabs %}

- С++

    ```cpp
    session
        .CreateSemaphore(
            "my-semaphore",  // semaphore name
            10               // semaphore limit
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

    Также при создании семафора можно передать строку, которая будет храниться вместе с семафором и возвращаться при его захвате:

    ```cpp
    session
        .CreateSemaphore(
            "my-semaphore",  // semaphore name
            10,              // semaphore limit
            "my-data"        // semaphore data
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

- Go

    ```go
    err := session.CreateSemaphore(ctx,
        "my-semaphore", // semaphore name
        10              // semaphore limit
    )
   ```

- Python

  В Python SDK семафор создаётся неявно при первом вызове `acquire()` в методе `session.semaphore(name, limit)`. Лимит указывается при создании объекта семафора.

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # семафор будет создан при первом acquire() с лимитом 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # семафор будет создан при первом acquire() с лимитом 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  const sem = session.semaphore("connections");
  await sem.create({
    limit: 10,
    data: new Uint8Array(),
  });
  ```

- Java

  Семафор создаётся явно методом `createSemaphore` у подключённой сессии. Можно передать пользовательские двоичные данные, хранящиеся вместе с семафором (`byte[] data`); вариант метода без параметра `data` эквивалентен передаче `null`. Если семафор с таким именем уже есть, операция завершится статусом «уже существует».

  ```java
  session.createSemaphore("my-semaphore", 10, new byte[] {0x00, 0x12})
      .join()
      .expectSuccess("create semaphore failed");
  ```

- Rust

  [`CoordinationSession::create_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.create_semaphore) принимает имя, лимит и произвольные байты `data`, хранимые у семафора.

  ```rust
  session.create_semaphore("my-semaphore", 10, vec![]).await?;

  // или с пользовательскими данными, хранимыми у семафора:
  session
      .create_semaphore("other-semaphore", 10, b"my-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Захват семафора {#acquire-semaphore}

Чтобы захватить семафор, клиент должен вызвать метод `AcquireSemaphore` и дождаться получения специального объекта `Lease`. Этот объект представляет собой подтверждение о том, что значение семафора было успешно увеличено и может считаться таковым до явного отпускания такого семафора или завершения сессии, в которой такое подтверждение было получено.

{% list tabs %}

- C++

    ```cpp
    session
        .AcquireSemaphore(
            "my-semaphore",                       // semaphore name
            TAcquireSemaphoreSettings().Count(5)  // value to increase semaphore by
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

    При захвате можно опционально передать структуру `TAcquireSemaphoreSettings` со следующими настройками:

    - `Count` - значение, на которое увеличивается семафор при захвате.
    - `Data` - дополнительные данные, которые можно положить в семафор.
    - `OnAccepted` - вызывается, когда операция встаёт в очередь (например, если семафор невозможно было захватить сразу).

      - Не будет вызвано, если семафор захватывается сразу.
      - Важно учитывать, что вызов может произойти параллельно с результатом `TFuture`.

    - `Timeout` - максимальное время, в течение которого операция может пролежать в очереди на сервере.

      - Операция вернёт `false`, если за время `Timeout` после добавления в очередь не удалось захватить семафор.
      - При `Timeout` установленном в 0 операция по смыслу работает как `TryAcquire`, т.е. семафор будет либо захвачен атомарно и операция вернёт `true`, либо операция вернёт `false` без использования очередей.

    - `Ephemeral` - если `true`, то имя является эфемерным семафором, такие семафоры автоматически создаются при первом `Acquire` и автоматически удаляются с последним `Release`.
    - `Shared()` - алиас для выставления `Count = 1`, захват семафора в shared режиме.
    - `Exclusive()` - алиас для выставления `Count = max`, захват семафора в exclusive режиме (для семафоров, созданных с лимитом `Max<ui64>()`).

- Go

    ```go
    lease, err := session.AcquireSemaphore(ctx,
        "my-semaphore",  // semaphore name
        5,              // value to increase semaphore by
    )
    ```

    Для отмены ожидания взятия семафора, достаточно отменить переданный в метод контекст `ctx`.

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        with semaphore:
            # семафор захвачен на 1 единицу (значение по умолчанию)
            pass
        # или вручную:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # работа с ресурсом
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        async with semaphore:
            # семафор захвачен на 1 единицу (значение по умолчанию)
            pass
        # или вручную:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # работа с ресурсом
        await semaphore.release()
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  {
    await using lease = await sem.acquire({ count: 1, data: new Uint8Array() });
    await doWork(lease.signal);
  } // lease.release() called automatically
  ```

- Java

  Захват выполняется через `acquireSemaphore` с именем семафора, числом токенов `count`, опциональными данными операции и таймаутом ожидания в очереди [java.time.Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html). Метод возвращает `CompletableFuture<Result<SemaphoreLease>>` (см. [Result](https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/Result.java) и [SemaphoreLease](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java)). Если семафор с указанным именем не существует, операция завершится исключением (см. javadoc метода).

  ```java
  import java.time.Duration;

  import tech.ydb.coordination.SemaphoreLease;
  import tech.ydb.core.Result;

  Result<SemaphoreLease> result = session
      .acquireSemaphore("my-semaphore", 5, Duration.ofSeconds(30))
      .join();

  result.getStatus().expectSuccess("cannot acquire semaphore");
  SemaphoreLease lease = result.getValue();
  ```

  Для **эфемерных** семафоров используйте `acquireEphemeralSemaphore` (флаг `exclusive` задаёт режим захвата); такие семафоры создаются при первом захвате и удаляются после последнего освобождения.

  В документации API указано: в один момент времени сессия может удерживать **только один** семафор; повторные вызовы для того же имени **заменяют** предыдущую операцию (например, чтобы уменьшить `count` или сменить таймаут).

- Rust

  [`acquire_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore) возвращает [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html). Таймаут ожидания в очереди, эфемерность и данные операции задаются через [`AcquireOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.AcquireOptionsBuilder.html) и [`acquire_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore_with_params).

  ```rust
  use std::time::Duration;
  use ydb::AcquireOptionsBuilder;

  let _lease = session.acquire_semaphore("my-semaphore", 5).await?;

  let opts = AcquireOptionsBuilder::default()
      .timeout(Duration::from_secs(30))
      .build()?;
  let _lease = session
      .acquire_semaphore_with_params("my-semaphore", 5, opts)
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

Взятое значение захваченного семафора можно снизить (но не увеличить), вновь вызвав для него метод `AcquireSemaphore` с меньшим значением.

### Обновление данных семафора {#update-semaphore}

С помощью метода `UpdateSemaphore` можно обновить (заменить) данные семафора, которые были привязаны при его создании.

{% list tabs %}

- C++

    ```cpp
    session
        .UpdateSemaphore(
            "my-semaphore",  // semaphore name
            "updated-data"   // new semaphore data
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

- Go

    ```go
    err := session.UpdateSemaphore(
        "my-semaphore",                                                          // semaphore name
        options.WithUpdateData([]byte("updated-data")),   // new semaphore data
    )
    ```

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.update(b"updated-data")
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.update(b"updated-data")
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  const sem = session.semaphore("connections");
  await sem.update({
    limit: 5,
    data: new Uint8Array(),
  });
  ```

- Java

  ```java
  session.updateSemaphore("my-semaphore", "updated-data".getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .join()
      .expectSuccess("update semaphore failed");
  ```

- Rust

  ```rust
  session
      .update_semaphore("my-semaphore", b"updated-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

Этот вызов не требует захвата семафора и не приводит к нему. Если требуется, чтобы данные обновлял только один конкретный клиент, то это необходимо явным образом обеспечить, например, захватив семафор, обновив данные и отпустив семафор обратно.

### Получение данных семафора {#describe-semaphore}

{% list tabs %}

- C++

    ```cpp
    session
        .DescribeSemaphore(
            "my-semaphore"  // semaphore name
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

    При получении информации о семафоре можно опционально передать структуру `TDescribeSemaphoreSettings` со следующими настройками:

    - `OnChanged` - вызывается один раз после изменения данных на сервере. C параметром `bool`, если `true` - то вызов произошёл из-за каких-то изменений, если `false` - то это ложный вызов и необходимо повторить `DescribeSemaphore` для восстановления подписки.
    - `WatchData` - вызывать `OnChanged` в случае изменения данных семафора.
    - `WatchOwners` - вызывать `OnChanged` в случае изменения владельцев семафора.
    - `IncludeOwners` - вернуть список владельцев в результатах.
    - `IncludeWaiters` - вернуть список ожидающих в результатах.

    Результат вызова представляет собой структуру со следующими полями:

    - `Name` - имя семафора.
    - `Data` - данные семафора.
    - `Count` - текущее значение семафора.
    - `Limit` - максимальное количество токенов, указанное при создании семафора.
    - `Owners` - список владельцев семафора.
    - `Waiters` - список ожидающих в очереди на семафоре.
    - `Ephemeral` - является ли семафор эфемерным.

    Поля `Owners` и `Waiters` в результате представляют собой список структур со следующими полями:

    - `OrderId` - порядковый номер операции захвата на семафоре. Может использоваться для идентификации, например если `OrderId` изменился, значит сессия сделала `ReleaseSemaphore` и новый `AcquireSemaphore`.
    - `SessionId` - идентификатор сессии, которая делала данный `AcquireSemaphore`.
    - `Timeout` - таймаут, с которым вызывался `AcquireSemaphore` для операций в очереди.
    - `Count` - запрошенное в `AcquireSemaphore` значение.
    - `Data` - данные, которые были указаны в `AcquireSemaphore`.

- Go

    ```go
    description, err := session.DescribeSemaphore(
        "my-semaphore"                                // semaphore name
        options.WithDescribeOwners(true), // to get list of owners
        options.WithDescribeWaiters(true), // to get list of waiters
    )
    ```

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        description = semaphore.describe()
        # description содержит: name, data, count, limit, owners, waiters, ephemeral
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        description = await semaphore.describe()
        # description содержит: name, data, count, limit, owners, waiters, ephemeral
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  ```javascript
  const sem = session.semaphore("connections");
  await sem.describe({
    owners: true,
    waiters: true,
  });
  ```

- Java

  Метод `describeSemaphore` принимает имя семафора и режим [DescribeSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/DescribeSemaphoreMode.java): только данные, со списком владельцев, со списком ожидающих или оба списка.

  ```java
  import tech.ydb.coordination.description.SemaphoreDescription;
  import tech.ydb.coordination.settings.DescribeSemaphoreMode;

  SemaphoreDescription description = session
      .describeSemaphore("my-semaphore", DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS)
      .join()
      .getValue();
  ```

  У элементов списков владельцев и ожидающих (`getOwnersList`, `getWaitersList`) доступны идентификатор сессии, таймаут, запрошенный `count`, данные операции и `orderId` (см. вложенный тип `SemaphoreDescription.Session` в исходниках).

  Для подписки на изменения используйте `watchSemaphore` с тем же режимом описания и [WatchSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/WatchSemaphoreMode.java) (данные, владельцы или оба). Объект [SemaphoreWatcher](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreWatcher.java) содержит снимок `SemaphoreDescription` и `getChangedFuture()` — `CompletableFuture<Result<SemaphoreChangedEvent>>` (см. [SemaphoreChangedEvent](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreChangedEvent.java), поля `isDataChanged`, `isOwnersChanged`). Future завершится при следующем событии; после уведомления для продолжения наблюдения вызовите `watchSemaphore` снова (см. [тесты](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/test/java/tech/ydb/coordination/CoordinationServiceTest.java)).

- Rust

  [`describe_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore) по умолчанию запрашивает владельцев и ожидающих. Набор флагов можно задать через [`DescribeOptions`](https://docs.rs/ydb/latest/ydb/struct.DescribeOptions.html) и [`describe_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore_with_params). Для подписки на изменения смотрите [`WatchOptions`](https://docs.rs/ydb/latest/ydb/struct.WatchOptions.html) в документации крейта.

  ```rust
  let description = session.describe_semaphore("my-semaphore").await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Освобождение семафора {#release-semaphore}

{% list tabs %}

- C++

    ```cpp
    session
        .ReleaseSemaphore(
            "my-semaphore"  // semaphore name
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

- Go

    Чтобы отпустить захваченный в сессии семафор, необходимо вызвать метод `Release` у объекта `Lease`.

    ```go
    err := lease.Release()
    ```

- Python

  В Python SDK семафор освобождается методом `release()` у объекта семафора. При использовании контекстного менеджера (`with` или `async with`) освобождение происходит автоматически при выходе из блока.

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # работа с ресурсом
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # работа с ресурсом
        await semaphore.release()
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  Чтобы отпустить захваченный в сессии семафор, необходимо вызвать метод `Release` у объекта `Lease`. Если взятие семафора было с использованием конструкции using, то при выходе из скоупа, семафор будет освобожден автоматически.

  ```javascript
  await lease.release();
  ```

- Java

  Освобождение — через [SemaphoreLease.release()](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java) (асинхронно, `CompletableFuture<Status>`).

  ```java
  lease.release().join().expectSuccess("release failed");
  ```

- Rust

  Вызовите [`Lease::release`](https://docs.rs/ydb/latest/ydb/struct.Lease.html#method.release) или просто завершите владение `Lease` — при уничтожении значения также отправляется освобождение на сервер.

  ```rust
  let lease = session.acquire_semaphore("my-semaphore", 1).await?;
  // …
  lease.release();
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Важные особенности

Операции `AcquireSemaphore` и `ReleaseSemaphore` являются идемпотентными. Если на семафоре был вызван `AcquireSemaphore`, повторные вызовы `AcquireSemaphore` изменяют только параметры захвата. Например, вызов `AcquireSemaphore` с `count=10` может добавить операцию в очередь. До или после успешного захвата можно повторно вызвать `AcquireSemaphore` с `count=9`, уменьшая количество захваченных единиц; новая операция заменит старую (которая завершится с кодом `ABORTED`, если она ещё не была успешно завершена). Позиция в очереди при этом не изменяется, несмотря на замену одной операции `AcquireSemaphore` на другую.

Операции `AcquireSemaphore` и `ReleaseSemaphore` возвращают `bool`, указывающий, изменила ли операция состояние семафора. Например, `AcquireSemaphore` вернёт `false`, если захват семафора не удался в течение времени `Timeout`, так как он был захвачен другим. Операция `ReleaseSemaphore` может вернуть `false`, если семафор не захвачен в текущей сессии.

Операцию `AcquireSemaphore`, находящуюся в очереди, можно завершить досрочно, вызвав `ReleaseSemaphore`. Независимо от количества вызовов `AcquireSemaphore` для конкретного семафора в одной сессии, освобождение происходит одним вызовом `ReleaseSemaphore`, то есть операции `AcquireSemaphore` и `ReleaseSemaphore` нельзя использовать как аналог `Acquire` или `Release` на рекурсивном мьютексе.

Операция `DescribeSemaphore` с флагами `WatchData` или `WatchOwners` создаёт подписку на изменения семафора. Любая более старая подписка на тот же семафор в сессии отменяется, вызывая `OnChanged(false)`. Рекомендуется игнорировать `OnChanged` от предыдущих вызовов `DescribeSemaphore`, если выполняется новый замещающий вызов, например, запоминая текущий id вызова.

Вызов `OnChanged(false)` может происходить не только из-за отмены новым `DescribeSemaphore`, но и по другим причинам, например, при временном разрыве соединения между grpc клиентом и сервером, при временном разрыве соединения между grpc сервером и текущим лидером сервиса, при изменении лидера сервиса, то есть при малейшем подозрении, что нотификация могла быть потеряна. Для восстановления подписки клиентский код должен выполнить новый вызов `DescribeSemaphore`, правильно обрабатывая ситуацию, что результат нового вызова может быть другим (например, если нотификация действительно была потеряна).

## Примеры

* [Распределённая блокировка](../../recipes/ydb-sdk/distributed-lock.md)
* [Выбор лидера](../../recipes/ydb-sdk/leader-election.md)
* [Обнаружение сервисов](../../recipes/ydb-sdk/service-discovery.md)
* [Публикация конфигурации](../../recipes/ydb-sdk/config-publication.md)