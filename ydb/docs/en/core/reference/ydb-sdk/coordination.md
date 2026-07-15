# Working with coordination nodes

This article describes how to use {{ ydb-short-name }} SDK to coordinate multiple instances of a client application by using [coordination nodes](../../concepts/datamodel/coordination-node.md) and the semaphores they contain.

## Creating a coordination node

Coordination nodes are created in {{ ydb-short-name }} databases in the same namespace as other schema objects, such as [tables](../../concepts/datamodel/table.md) and [topics](../../concepts/datamodel/topic.md).

{% list tabs %}

- C++

  ```cpp
  TClient client(driver);
  auto status = client
      .CreateNode("/path/to/mynode")
      .ExtractValueSync();
  Y_ABORT_UNLESS(status.IsSuccess());
  ```


  When creating, you can optionally specify `TNodeSettings` with the following settings:

  - `ReadConsistencyMode` - default `RELAXED`, which allows reading a not‑most‑fresh value when the leader changes. Optionally you can enable `STRICT` read mode, where all reads go through the consensus algorithm and guarantee returning the most recent value, but become significantly more expensive.
  - `AttachConsistencyMode` - default `STRICT`, which requires using the consensus algorithm when restoring a session. Optionally you can enable `RELAXED` session‑recovery mode in case of failures, which disables this requirement. A relaxed mode may be needed with a very large number of clients, allowing session restoration without going through consensus, which does not affect overall correctness but may increase reading stale values during leader changes and also cause session expiration in case of issues.
  - `SelfCheckPeriod` (default 1 second) – the interval at which the service checks its own liveness. Changing it is not recommended except in special cases.

    - The larger the specified value, the lower the load on the server, but the longer the possible delay between a leader change and how quickly the service learns about it.
    - The smaller the specified value, the higher the load on the server and the greater responsiveness in detecting problems, but false positives may be generated when the service mistakenly detects issues.
  - `SessionGracePeriod` (default 10 seconds) – the period during which a new leader does not close open sessions, extending them.

    - The smaller the value, the narrower the window during which sessions from non‑existent clients, which did not report their disappearance during a leader change, will hold semaphores and block other clients.
    - The smaller the value, the higher the chance of false triggers, where a live leader may shut down as a precaution because it cannot be sure that this period has not elapsed on the new leader.
    - It must be strictly greater than `SelfCheckPeriod`.

- Go

  ```go
  err := db.Coordination().CreateNode(ctx,
      "/path/to/mynode",
  )
  ```

- Java

  To work with coordination nodes, add the Maven artifact `ydb-sdk-coordination` (module `tech.ydb.coordination.*`). Coordination nodes are needed when multiple instances of an application must coordinate access to resources – see the [coordination node](../../concepts/datamodel/coordination-node.md) section for more details.

  Below is a complete example: connecting via `GrpcTransport`, creating a node, and checking via `describeNode`.


  ```java
  import tech.ydb.coordination.CoordinationClient;
  import tech.ydb.coordination.description.NodeConfig;
  import tech.ydb.core.grpc.GrpcTransport;

  public class CreateCoordinationNodeExample {

      private static final String NODE_PATH_SUFFIX = "/path/to/mynode";

      public static void main(String[] args) {
          // Connection string from environment variable or default local YDB
          String connectionString = System.getenv().getOrDefault(
                  "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

          try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
               CoordinationClient client = CoordinationClient.newClient(transport).build()) {

              // Full path to the node = database path + node name in the namespace
              String nodePath = client.getDatabase() + NODE_PATH_SUFFIX;

              // Create a coordination node with default settings
              client.createNode(nodePath).join().expectSuccess("не удалось создать узел");

              // Verify that the node is created: read its configuration
              NodeConfig config = client.describeNode(nodePath).join().getValue();
              System.out.println("Узел создан: " + nodePath);
              System.out.println("SelfCheckPeriod: " + config.getSelfCheckPeriod());
              System.out.println("SessionGracePeriod: " + config.getSessionGracePeriod());
          }
      }
  }
  ```


  If needed, set the node configuration via [NodeConfig](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/NodeConfig.java), using the `NodeConfig.create().with…` chain. Available parameters: periods `SelfCheckPeriod` and `SessionGracePeriod`, read and session-connection consistency modes (`readConsistencyMode`, `attachConsistencyMode`), rate-limiter counters mode (`rateLimiterCountersMode`). Default values match the description for C++ (see above). The prepared `NodeConfig` is passed to `CoordinationNodeSettings` and `createNode(nodePath, settings)`.

  Additionally, `alterNode` (configuration change) and `dropNode` (node removal) are available.

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

  The coordination client is returned from [`Client::coordination_client`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.coordination_client). A node is created via [`CoordinationClient::create_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_node) with a path and [`NodeConfig`](https://docs.rs/ydb/latest/ydb/struct.NodeConfig.html) (via [`NodeConfigBuilder`](https://docs.rs/ydb/latest/ydb/struct.NodeConfigBuilder.html)). Also available are [`alter_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.alter_node), [`drop_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.drop_node), and [`describe_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.describe_node). Full example — [`mutex.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/mutex.rs).


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

## Working with sessions {#session}

### Creating a session {#create-session}

To get started, the client must establish a session within which it will perform all operations with the coordination node.

{% list tabs %}

- C++

  ```cpp
  TClient client(driver);
  const TSession& session = client
     .StartSession("/path/to/mynode")
     .ExtractValueSync()
     .ExtractResult();
  ```


  When establishing a session, you can optionally pass a `TSessionSettings` structure with the following settings:

  - `Description` – a textual description of the session, displayed in internal interfaces and useful for diagnosing problems.
  - `OnStateChanged` – called on important changes during the session’s lifecycle, passing the corresponding state:

    - `ATTACHED` – the session is connected and operating in normal mode.
    - `DETACHED` – the session temporarily lost connection to the service but can still be restored.
    - `EXPIRED` – the session lost connection to the service and cannot be restored.
  - `OnStopped` – called when the session stops attempting to restore the connection to the service, which can be useful for establishing a new connection.
  - `Timeout` – the maximum timeout during which the session can be restored after losing connection to the service.

- Go

  ```go
  session, err := db.Coordination().CreateSession(ctx,
      "/path/to/mynode", // name of the Coordination Node in the database
  )
  ```

- Java

  Before working with [semaphores](../../concepts/datamodel/coordination-node.md#semaphore) the client opens a session (see [CoordinationSession](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/CoordinationSession.java)): calling `createSession` creates a session object, and `connect()` establishes a bidirectional gRPC stream with a node. Retry parameters and connection timeout are set in [CoordinationSessionSettings](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/CoordinationSessionSettings.java) (`withConnectTimeout`, `withRetryPolicy`, `withExecutor`).

  Typical scenario: after a successful `connect()` you perform semaphore operations, then close the session via `close()` (conveniently — try-with-resources). While the session is active, the SDK automatically retries the connection on network failures according to the settings.


  ```java
  import tech.ydb.coordination.CoordinationClient;
  import tech.ydb.coordination.CoordinationSession;
  import tech.ydb.coordination.settings.CoordinationSessionSettings;
  import tech.ydb.core.grpc.GrpcTransport;

  public class CoordinationSessionExample {

      private static final String NODE_PATH_SUFFIX = "/path/to/mynode";

      public static void main(String[] args) {
          String connectionString = System.getenv().getOrDefault(
                  "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

          try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
               CoordinationClient client = CoordinationClient.newClient(transport).build()) {

              String nodePath = client.getDatabase() + NODE_PATH_SUFFIX;
              client.createNode(nodePath).join().expectSuccess("не удалось создать узел");

              // try-with-resources guarantees the call to close() and stopping the thread with the node
              try (CoordinationSession session = client.createSession(
                      nodePath,
                      CoordinationSessionSettings.newBuilder().build())) {

                  // Establish a connection to the coordination node
                  session.connect().join().expectSuccess("не удалось подключить сессию");
                  System.out.println("Сессия подключена, id=" + session.getId());

                  // ... semaphore operations (see the “Working with semaphores” section) ...

              } // session.close() — explicit session termination
          }
      }
  }
  ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # working with the session
        pass
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # working with the session
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

  The session is created by [`CoordinationClient::create_session`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_session) with a path to a node and [`SessionOptions`](https://docs.rs/ydb/latest/ydb/struct.SessionOptions.html) ([`SessionOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.SessionOptionsBuilder.html): timeout, description, etc.). The thread with the node is started inside the session constructor; there is no separate `connect` call as in Java.


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

### Session termination control {#session-control}

Your client application must monitor the session state, because it can rely on the state of acquired semaphores only while the session is active. When the session ends by client or server initiative, the client can no longer be sure that other clients in the cluster have not acquired its semaphores and changed their state.

{% list tabs %}

- C++

  In the C++ SDK, a background session is maintained and automatically restores the connection to the {{ ydb-short-name }} cluster.

- Go

  In the Go SDK, a session context `session.Context()` is used to track such situations; it ends together with the session. The SDK independently handles transport-level errors and restores the connection to the service, attempting to restore the session when possible. Thus, you only need to monitor the session context to react promptly to its loss.

- Python

  In the Python SDK, the session automatically restores the connection to the {{ ydb-short-name }} cluster on failures. It is recommended to use a context manager (`with` or `async with`) to ensure the session is closed when exiting the block. When working with semaphores via a context manager (`with session.semaphore(name)` or `async with session.semaphore(name)`), the semaphore is automatically released upon exiting the block, and the session is closed when the context ends.

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  In the JS SDK, a signal `session.signal` is used to track such situations; it is aborted together with the session. The SDK independently handles transport-level errors and restores the connection to the service, attempting to restore the session when possible. Thus, you only need to monitor the session signal to avoid performing actions when the session has been closed or expired.

  The JavaScript SDK also provides a method to obtain a new session when the old one is lost, and this approach is recommended for long-term use `for await (session of client.openSession()) { session.signal }`.

- Java

  Close the session (`close()`) when your scenario has finished: this explicitly releases the connection to the node. While the session remains open, the SDK automatically retries the connection on network failures according to `CoordinationSessionSettings`. Hold a semaphore only for the duration of solving the user task and release it via `SemaphoreLease.release()` when the resource is no longer needed.

- Rust

  With [`CoordinationSession`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html) call [`alive`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.alive): it returns [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) — it is cancelled when the session ends (similar to Go's context tracking). When releasing [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html) or when `Drop` the session, the semaphore release is sent to the server in the background.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Working with semaphores {#semaphore}

### Creating a semaphore {#create-semaphore}

When creating a semaphore you can specify its limit. The limit defines the maximum value to which it can be increased. Calls that try to increase the semaphore value beyond this limit will wait until their increase requests can be fulfilled so that the semaphore value does not exceed its limit.

{% list tabs %}

- C++

  ```cpp
  session
      .CreateSemaphore(
          "my-semaphore",  // semaphore name
          10               // semaphore limit
      )
      .ExtractValueSync()
      .ExtractResult();
  ```


  You can also pass a string when creating a semaphore, which will be stored with the semaphore and returned when it is acquired:


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

  In the Python SDK the semaphore is created implicitly on the first `acquire()` call in the `session.semaphore(name, limit)` method. The limit is specified when creating the semaphore object.

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # the semaphore will be created on the first acquire() with a limit of 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # the semaphore will be created on the first acquire() with a limit of 10
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

  Below is a complete example of a semaphore lifecycle [semaphore](../../concepts/datamodel/coordination-node.md#semaphore): creating a node and session, creating the semaphore, acquiring, updating and reading data, and releasing. A persistent semaphore must be created explicitly (`createSemaphore`); ephemeral semaphores are created on first acquire (see the “Acquiring a semaphore” section).


  ```java
  import java.nio.charset.StandardCharsets;
  import java.time.Duration;

  import tech.ydb.coordination.CoordinationClient;
  import tech.ydb.coordination.CoordinationSession;
  import tech.ydb.coordination.SemaphoreLease;
  import tech.ydb.coordination.description.SemaphoreDescription;
  import tech.ydb.coordination.settings.DescribeSemaphoreMode;
  import tech.ydb.core.grpc.GrpcTransport;

  public class CoordinationSemaphoreExample {

      private static final String NODE_PATH_SUFFIX = "/path/to/mynode";
      private static final String SEMAPHORE_NAME = "my-semaphore";

      public static void main(String[] args) {
          String connectionString = System.getenv().getOrDefault(
                  "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

          try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
               CoordinationClient client = CoordinationClient.newClient(transport).build()) {

              String nodePath = client.getDatabase() + NODE_PATH_SUFFIX;

              // 1. Create a coordination node
              client.createNode(nodePath).join().expectSuccess("не удалось создать узел");

              try (CoordinationSession session = client.createSession(nodePath)) {
                  session.connect().join().expectSuccess("не удалось подключить сессию");

                  byte[] initialData = "my-data".getBytes(StandardCharsets.UTF_8);

                  // 2. Create a semaphore with a limit of 10 and initial data
                  session.createSemaphore(SEMAPHORE_NAME, 10, initialData)
                          .join().expectSuccess("не удалось создать семафор");

                  // 3. Acquire 5 tokens; wait in the queue for no more than 30 seconds
                  SemaphoreLease lease = session
                          .acquireSemaphore(SEMAPHORE_NAME, 5, Duration.ofSeconds(30))
                          .join().getValue();

                  try {
                      // 4. Update the data attached to the semaphore
                      byte[] updatedData = "updated-data".getBytes(StandardCharsets.UTF_8);
                      session.updateSemaphore(SEMAPHORE_NAME, updatedData)
                              .join().expectSuccess("не удалось обновить данные семафора");

                      // 5. Read the current state of the semaphore
                      SemaphoreDescription description = session
                              .describeSemaphore(SEMAPHORE_NAME, DescribeSemaphoreMode.DATA_ONLY)
                              .join().getValue();

                      System.out.println("Имя: " + description.getName());
                      System.out.println("Лимит: " + description.getLimit());
                      System.out.println("Захвачено: " + description.getCount());
                      System.out.println("Данные: "
                              + new String(description.getData(), StandardCharsets.UTF_8));

                  } finally {
                      // 6. Release the acquired tokens
                      lease.release().join().expectSuccess("не удалось освободить семафор");
                  }
              }
          }
      }
  }
  ```


  If a semaphore with that name already exists, `createSemaphore` returns a “already exists” status. The version without the `data` parameter is equivalent to passing `null`.

- Rust

  [`CoordinationSession::create_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.create_semaphore) takes a name, a limit, and arbitrary bytes `data` stored in the semaphore.


  ```rust
  session.create_semaphore("my-semaphore", 10, vec![]).await?;

  // or with custom data stored in the semaphore:
  session
      .create_semaphore("other-semaphore", 10, b"my-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Acquiring a semaphore {#acquire-semaphore}

To acquire a semaphore, the client must call the `AcquireSemaphore` method and wait for the special `Lease` object. This object serves as confirmation that the semaphore value was successfully increased and can be considered as such until the semaphore is explicitly released or the session in which the confirmation was obtained ends.

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


  When acquiring, you can optionally pass a `TAcquireSemaphoreSettings` structure with the following settings:

  - `Count` – the amount by which the semaphore is increased on acquire.
  - `Data` – additional data that can be stored in the semaphore.
  - `OnAccepted` – called when the operation is queued (for example, if the semaphore could not be acquired immediately).

    - Will not be called if the semaphore is acquired immediately.
    - Note that the call may occur concurrently with the result `TFuture`.
  - `Timeout` – the maximum time the operation may remain in the server queue.

    - The operation returns `false` if the semaphore could not be acquired within `Timeout` after being queued.
    - When `Timeout` is set to 0, the operation effectively works like `TryAcquire`, i.e., the semaphore will be acquired atomically and the operation returns `true`, or the operation returns `false` without using queues.
  - `Ephemeral` – if `true`, the name is an ephemeral semaphore; such semaphores are created automatically on the first `Acquire` and automatically removed with the last `Release`.
  - `Shared()` – alias for setting `Count = 1`, acquiring the semaphore in shared mode.
  - `Exclusive()` – alias for setting `Count = max`, acquiring the semaphore in exclusive mode (for semaphores created with a limit of `Max<ui64>()`).

- Go

  ```go
  lease, err := session.AcquireSemaphore(ctx,
      "my-semaphore",  // semaphore name
      5,              // value to increase semaphore by
  )
  ```


  To cancel waiting for a semaphore acquisition, simply cancel the `ctx` context passed to the method.

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        with semaphore:
            # semaphore acquired for 1 unit (default value)
            pass
        # or manually:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # working with the resource
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        async with semaphore:
            # semaphore acquired for 1 unit (default value)
            pass
        # or manually:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # working with the resource
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

  Acquisition is performed via `acquireSemaphore` (full example – see the “Creating a semaphore” section). The method takes the semaphore name, the number of tokens `count`, optional operation data, and a queue wait timeout [java.time.Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html). It returns `CompletableFuture<Result<SemaphoreLease>>` (see [Result](https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/Result.java) and [SemaphoreLease](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java)). If a semaphore with the specified name does not exist, the operation ends with an exception.

  For **ephemeral** semaphores, use `acquireEphemeralSemaphore` (the `exclusive` flag sets the acquisition mode); such semaphores are created on first acquire and removed after the final release.

  At any given time a session can hold **only one** semaphore; subsequent calls for the same name **replace** the previous operation (for example, to decrease `count` or change the timeout).

- Rust

  [`acquire_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore) returns [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html). The queue wait timeout, ephemerality, and operation data are set via [`AcquireOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.AcquireOptionsBuilder.html) and [`acquire_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore_with_params).


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

You can decrease (but not increase) the acquired semaphore's value by calling its method `AcquireSemaphore` again with a smaller value.

### Updating semaphore data {#update-semaphore}

You can update (replace) the semaphore data bound at its creation using the method `UpdateSemaphore`.

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

  Data update — the method `updateSemaphore` (step 4 in the “Creating a semaphore” example). The call does not require acquiring the semaphore and does not affect it.

- Rust

  ```rust
  session
      .update_semaphore("my-semaphore", b"updated-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

This call does not require acquiring the semaphore and does not affect it. If you need the data to be updated by only a single client, you must ensure this explicitly, for example by acquiring the semaphore, updating the data, and releasing the semaphore back.

### Getting semaphore data {#describe-semaphore}

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


  When retrieving semaphore information, you can optionally pass a `TDescribeSemaphoreSettings` structure with the following settings:

  - `OnChanged` – called once after data changes on the server. With the `bool` parameter, if `true` – the call occurred because of some changes; if `false` – it is a spurious call and you need to repeat `DescribeSemaphore` to restore the subscription.
  - `WatchData` – invoke `OnChanged` when the semaphore data changes.
  - `WatchOwners` – invoke `OnChanged` when the semaphore owners change.
  - `IncludeOwners` – return the list of owners in the results.
  - `IncludeWaiters` – return the list of waiters in the results.

  The call result is a structure with the following fields:

  - `Name` – semaphore name.
  - `Data` – semaphore data.
  - `Count` – current semaphore value.
  - `Limit` – maximum number of tokens specified when the semaphore was created.
  - `Owners` – list of semaphore owners.
  - `Waiters` – list of waiters in the semaphore queue.
  - `Ephemeral` – indicates whether the semaphore is ephemeral.

  The `Owners` and `Waiters` fields in the result are lists of structures with the following fields:

  - `OrderId` – sequential number of the acquire operation on the semaphore. It can be used for identification, for example if `OrderId` changed, it means the session performed `ReleaseSemaphore` and a new `AcquireSemaphore`.
  - `SessionId` – identifier of the session that performed this `AcquireSemaphore`.
  - `Timeout` – timeout with which `AcquireSemaphore` was called for queued operations.
  - `Count` – value requested in `AcquireSemaphore`.
  - `Data` – data that were specified in `AcquireSemaphore`.

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
        # description contains: name, data, count, limit, owners, waiters, ephemeral
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        description = await semaphore.describe()
        # description contains: name, data, count, limit, owners, waiters, ephemeral
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

  Reading semaphore state — the method `describeSemaphore` (step 5 in the “Creating a semaphore” example). It takes the semaphore name and a mode [DescribeSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/DescribeSemaphoreMode.java): data only, with owners list, with waiters list, or both lists.

  Elements of the owners and waiters lists (`getOwnersList`, `getWaitersList`) provide the session identifier, timeout, requested `count`, operation data, and `orderId` (see the nested type `SemaphoreDescription.Session` in the source).

  To subscribe to changes, use `watchSemaphore` with the same description mode and [WatchSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/WatchSemaphoreMode.java) (data, owners, or both). The [SemaphoreWatcher](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreWatcher.java) object contains a snapshot of `SemaphoreDescription` and `getChangedFuture()` — `CompletableFuture<Result<SemaphoreChangedEvent>>` (see [SemaphoreChangedEvent](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreChangedEvent.java), fields `isDataChanged`, `isOwnersChanged`). The Future completes on the next event; after notification, call `watchSemaphore` again to continue watching (see [tests](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/test/java/tech/ydb/coordination/CoordinationServiceTest.java)).

- Rust

  [`describe_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore) by default requests owners and waiters. You can set the flag set via [`DescribeOptions`](https://docs.rs/ydb/latest/ydb/struct.DescribeOptions.html) and [`describe_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore_with_params). To subscribe to changes, see [`WatchOptions`](https://docs.rs/ydb/latest/ydb/struct.WatchOptions.html) in the crate documentation.


  ```rust
  let description = session.describe_semaphore("my-semaphore").await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Releasing a semaphore {#release-semaphore}

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

  To release a semaphore acquired in a session, you need to call the `Release` method on the `Lease` object.


  ```go
  err := lease.Release()
  ```

- Python

  In the Python SDK, the semaphore is released by the `release()` method on the semaphore object. When using a context manager (`with` or `async with`), the release happens automatically when exiting the block.

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # working with the resource
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # working with the resource
        await semaphore.release()
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  To release a semaphore acquired in a session, you need to call the `Release` method on the `Lease` object. If the semaphore was taken using a `using` construct, it will be released automatically when exiting the scope.


  ```javascript
  await lease.release();
  ```

- Java

  Release is performed via [SemaphoreLease.release()](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java) (step 6 in the “Creating a semaphore” example). The method is asynchronous and returns `CompletableFuture<Status>`.

- Rust

  Call [`Lease::release`](https://docs.rs/ydb/latest/ydb/struct.Lease.html#method.release) or simply relinquish ownership of `Lease` — when the value is dropped, a release is also sent to the server.


  ```rust
  let lease = session.acquire_semaphore("my-semaphore", 1).await?;
  // …
  lease.release();
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Important features

Operations `AcquireSemaphore` and `ReleaseSemaphore` are idempotent. If `AcquireSemaphore` was called on the semaphore, subsequent calls to `AcquireSemaphore` only change the acquisition parameters. For example, calling `AcquireSemaphore` with `count=10` may add an operation to the queue. Before or after a successful acquisition you can call `AcquireSemaphore` again with `count=9`, decreasing the number of acquired units; the new operation will replace the old one (which will finish with code `ABORTED` if it has not yet completed successfully). The position in the queue does not change, despite replacing one operation `AcquireSemaphore` with another.

The `AcquireSemaphore` and `ReleaseSemaphore` operations return `bool` indicating whether the operation changed the semaphore state. For example, `AcquireSemaphore` returns `false` if acquiring the semaphore fails within `Timeout` because it was held by another. The `ReleaseSemaphore` operation may return `false` if the semaphore is not held in the current session.

You can complete the queued operation `AcquireSemaphore` early by calling `ReleaseSemaphore`. Regardless of the number of `AcquireSemaphore` calls for a particular semaphore in a single session, release occurs with a single `ReleaseSemaphore` call, i.e., operations `AcquireSemaphore` and `ReleaseSemaphore` cannot be used as equivalents of `Acquire` or `Release` on a recursive mutex.

The `DescribeSemaphore` operation with flags `WatchData` or `WatchOwners` creates a subscription to semaphore changes. Any older subscription to the same semaphore in the session is cancelled, triggering `OnChanged(false)`. It is recommended to ignore `OnChanged` from previous `DescribeSemaphore` calls if a new overriding call is made, for example by remembering the current call id.

The `OnChanged(false)` call can occur not only due to cancellation by a new `DescribeSemaphore`, but also for other reasons, such as a temporary connection break between the gRPC client and server, a temporary break between the gRPC server and the current service leader, or a change of the service leader—that is, at the slightest suspicion that a notification may have been lost. To restore the subscription, client code should make a new `DescribeSemaphore` call, correctly handling the possibility that the result of the new call may differ (for example, if the notification was indeed lost).

## Examples

* [Distributed lock](../../recipes/ydb-sdk/distributed-lock.md)
* [Leader election](../../recipes/ydb-sdk/leader-election.md)
* [Service discovery](../../recipes/ydb-sdk/service-discovery.md)
* [Configuration publishing](../../recipes/ydb-sdk/config-publication.md)
