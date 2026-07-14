# Working with coordination nodes

This article describes how to use the {{ ydb-short-name }} SDK to coordinate the work of multiple client application instances using [coordination nodes](../../concepts/datamodel/coordination-node.md) and the semaphores within them.

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

  - `ReadConsistencyMode` - defaults to `RELAXED`, which allows reading a value that is not the most recent in case of a leader change. Optionally, you can enable `STRICT` read mode, in which all reads go through the consensus algorithm and guarantee returning the most recent value, but become significantly more expensive.
  - `AttachConsistencyMode` - defaults to `STRICT`, which means mandatory use of the consensus algorithm when restoring a session. Optionally, you can enable `RELAXED` session recovery mode in case of failures, which disables this requirement. The relaxed mode may be needed when there is a very large number of clients, allowing session recovery without going through consensus, which does not affect overall correctness but may exacerbate reading a value that is not the most recent during a leader change, as well as session obsolescence in case of problems.
  - `SelfCheckPeriod` (defaults to 1 second) - the frequency with which the service performs its own liveness checks. It is not recommended to change it except in special cases.

    - The larger the specified value, the lower the load on the server, but the longer the possible delay between a leader change and how quickly the service itself learns about it.
    - The smaller the specified value, the higher the load on the server and the greater the responsiveness in detecting problems, but false positives may occur when the service incorrectly detects problems.
  - `SessionGracePeriod` (defaults to 10 seconds) - the period during which a new leader does not close open sessions, extending them.

    - The smaller the value, the smaller the window when sessions from non-existent clients that did not manage to report their disappearance during a leader change will hold semaphores and interfere with other clients.
    - The smaller the value, the higher the probability of false positives, when a live leader may terminate for safety, as it will not be sure that this period has not expired for the new leader.
    - Must be strictly greater than `SelfCheckPeriod`.

- Go

  ```go
  err := db.Coordination().CreateNode(ctx,
      "/path/to/mynode",
  )
  ```

- Java

  To work with coordination nodes, add the Maven artifact `ydb-sdk-coordination` (module `tech.ydb.coordination.*`). Coordination nodes are needed when multiple application instances must coordinate access to resources — see more in the [Coordination node](../../concepts/datamodel/coordination-node.md) section.

  Below is a complete example: connecting via `GrpcTransport`, creating a node, and checking via `describeNode`.


  ```java
  import tech.ydb.coordination.CoordinationClient;
  import tech.ydb.coordination.description.NodeConfig;
  import tech.ydb.core.grpc.GrpcTransport;

  public class CreateCoordinationNodeExample {

      private static final String NODE_PATH_SUFFIX = "/path/to/mynode";

      public static void main(String[] args) {
          // Connection string from environment variable or local YDB by default
          String connectionString = System.getenv().getOrDefault(
                  "YDB_CONNECTION_STRING", "grpc://localhost:2136/local");

          try (GrpcTransport transport = GrpcTransport.forConnectionString(connectionString).build();
               CoordinationClient client = CoordinationClient.newClient(transport).build()) {

              // Full path to node = database path + node name in namespace
              String nodePath = client.getDatabase() + NODE_PATH_SUFFIX;

              // Create a coordination node with default settings
              client.createNode(nodePath).join().expectSuccess("не удалось создать узел");

              // Check that the node is created: read its configuration
              NodeConfig config = client.describeNode(nodePath).join().getValue();
              System.out.println("Узел создан: " + nodePath);
              System.out.println("SelfCheckPeriod: " + config.getSelfCheckPeriod());
              System.out.println("SessionGracePeriod: " + config.getSessionGracePeriod());
          }
      }
  }
  ```


  If necessary, set the node configuration through [NodeConfig](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/NodeConfig.java) using the chain `NodeConfig.create().with…`. Available parameters: periods `SelfCheckPeriod` and `SessionGracePeriod`, read consistency and session connection modes (`readConsistencyMode`, `attachConsistencyMode`), rate limiter counter mode (`rateLimiterCountersMode`). Default values match the description for C++ (see above). The ready `NodeConfig` is passed to `CoordinationNodeSettings` and to `createNode(nodePath, settings)`.

  Additionally available are `alterNode` (configuration change) and `dropNode` (node deletion).

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

  Coordination client is returned from [`Client::coordination_client`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.coordination_client). A node is created via [`CoordinationClient::create_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_node) with a path and [`NodeConfig`](https://docs.rs/ydb/latest/ydb/struct.NodeConfig.html) (via [`NodeConfigBuilder`](https://docs.rs/ydb/latest/ydb/struct.NodeConfigBuilder.html)). Also available are [`alter_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.alter_node), [`drop_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.drop_node), [`describe_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.describe_node). A full example is [`mutex.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/mutex.rs).


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

  - `Description` - a text description of the session, displayed in internal interfaces and can be useful for diagnosing problems.
  - `OnStateChanged` - called on important changes in the session lifecycle, passing the corresponding state:

    - `ATTACHED` - the session is connected and operating normally.
    - `DETACHED` - the session has temporarily lost connection with the service, but can still be restored.
    - `EXPIRED` - the session has lost connection with the service and cannot be restored.
  - `OnStopped` - called when the session stops trying to reconnect to the service, which can be useful for establishing a new connection.
  - `Timeout` - the maximum timeout during which the session can be restored after losing connection with the service.

- Go

  ```go
  session, err := db.Coordination().CreateSession(ctx,
      "/path/to/mynode", // Coordination Node name in the database
  )
  ```

- Java

  Before working with [semaphores](../../concepts/datamodel/coordination-node.md#semaphore), the client opens a session (see [CoordinationSession](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/CoordinationSession.java)): the `createSession` call creates a session object, and `connect()` establishes a bidirectional gRPC stream with the node. Retry parameters and connection timeout are set in [CoordinationSessionSettings](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/CoordinationSessionSettings.java) (`withConnectTimeout`, `withRetryPolicy`, `withExecutor`).

  Typical scenario: after a successful `connect()`, you perform operations with semaphores, then close the session via `close()` (conveniently — try-with-resources). While the session is active, the SDK automatically retries the connection according to the settings in case of network failures.


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

              // try-with-resources guarantees calling close() and stopping the thread with the node
              try (CoordinationSession session = client.createSession(
                      nodePath,
                      CoordinationSessionSettings.newBuilder().build())) {

                  // Establish a connection to the coordination node
                  session.connect().join().expectSuccess("не удалось подключить сессию");
                  System.out.println("Сессия подключена, id=" + session.getId());

                  // ... operations with semaphores (see section 'Working with semaphores') ...

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
        # working with session
        pass
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # working with session
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

  The session is created by [`CoordinationClient::create_session`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_session) with a path to the node and [`SessionOptions`](https://docs.rs/ydb/latest/ydb/struct.SessionOptions.html) ([`SessionOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.SessionOptionsBuilder.html): timeout, description, etc.). The stream with the node is started inside the session constructor; there is no separate call to `connect`, as in Java.


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

### Controlling session termination {#session-control}

The client application must monitor the session state, as it can rely on the state of acquired semaphores only while the session is active. When the session terminates at the client's or server's initiative, the client can no longer be sure that other clients in the cluster have not acquired its semaphores and changed their state.

{% list tabs %}

- C++

  In the C++ SDK, an established session maintains and automatically restores communication with the {{ ydb-short-name }} cluster in the background.

- Go

  In the Go SDK, the session context `session.Context()` is used to track such situations; it terminates together with the session. The SDK independently handles transport-level errors and restores the connection to the service, attempting to recover the session if possible. Thus, the client only needs to monitor the session context to react promptly to its loss.

- Python

  In the Python SDK, the session automatically restores communication with the {{ ydb-short-name }} cluster in case of failures. It is recommended to use a context manager (`with` or `async with`) to guarantee session closure when exiting the block. When working with semaphores via a context manager (`with session.semaphore(name)` or `async with session.semaphore(name)`), the semaphore is automatically released when exiting the block, and the session is released when the context is closed.

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  In the JS SDK, the signal `session.signal` is used to track such situations; it is interrupted together with the session. The SDK independently handles transport-level errors and restores the connection to the service, attempting to recover the session if possible. Thus, the client only needs to monitor the session signal to avoid performing actions when the session has been closed or expired.

  Also, the JavaScript SDK has a method for obtaining a new session when the old one is lost, and this approach is recommended for long-term use of `for await (session of client.openSession()) { session.signal }`.

- Java

  Terminate the session (`close()`) when your scenario has completed: this explicitly releases the connection to the node. While the session is not closed, the SDK automatically retries the connection according to `CoordinationSessionSettings` in case of network failures. Hold the semaphore only for the duration of solving the user task and release it via `SemaphoreLease.release()` when the resource is no longer needed.

- Rust

  Call [`alive`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.alive) on [`CoordinationSession`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html): it returns [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) — when the session ends, it is canceled (similar to context tracking in Go). When [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html) is released or when the session `Drop`, the semaphore release goes to the server in the background.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Working with semaphores {#semaphore}

### Creating a semaphore {#create-semaphore}

When creating a semaphore, you can specify its limit. The limit determines the maximum value to which it can be increased. Calls that try to increase the semaphore value above this limit will start waiting until their increase requests can be fulfilled, so that the semaphore value does not exceed its limit.

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


  Also, when creating a semaphore, you can pass a string that will be stored with the semaphore and returned when it is acquired:


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

  In the Python SDK, a semaphore is created implicitly on the first call to `acquire()` in the `session.semaphore(name, limit)` method. The limit is specified when creating the semaphore object.

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # semaphore will be created on first acquire() with limit 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # semaphore will be created on first acquire() with limit 10
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

  Below is a complete example of a [semaphore](../../concepts/datamodel/coordination-node.md#semaphore) lifecycle: creating a node and session, creating a semaphore, acquiring, updating and reading data, releasing. A persistent semaphore must be created explicitly (`createSemaphore`); ephemeral semaphores are created on first acquisition (see the "Acquiring a semaphore" section).


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

                  // 2. Create a semaphore with limit 10 and initial data
                  session.createSemaphore(SEMAPHORE_NAME, 10, initialData)
                          .join().expectSuccess("не удалось создать семафор");

                  // 3. Acquire 5 tokens; wait in queue no more than 30 seconds
                  SemaphoreLease lease = session
                          .acquireSemaphore(SEMAPHORE_NAME, 5, Duration.ofSeconds(30))
                          .join().getValue();

                  try {
                      // 4. Update data attached to the semaphore
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


  If a semaphore with that name already exists, `createSemaphore` returns a status of 'already exists'. The variant without the `data` parameter is equivalent to passing `null`.

- Rust

  [`CoordinationSession::create_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.create_semaphore) takes a name, a limit, and arbitrary bytes `data` stored with the semaphore.


  ```rust
  session.create_semaphore("my-semaphore", 10, vec![]).await?;

  // or with user data stored at the semaphore:
  session
      .create_semaphore("other-semaphore", 10, b"my-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Acquiring a semaphore {#acquire-semaphore}

To acquire a semaphore, the client must call the `AcquireSemaphore` method and wait to receive a special `Lease` object. This object represents confirmation that the semaphore value has been successfully increased and can be considered as such until the semaphore is explicitly released or the session in which such confirmation was received ends.

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

  - `Count` - the value by which the semaphore is increased upon acquisition.
  - `Data` - additional data that can be placed in the semaphore.
  - `OnAccepted` - called when the operation is queued (for example, if the semaphore could not be acquired immediately).

    - Will not be called if the semaphore is acquired immediately.
    - It is important to note that the call may occur concurrently with the `TFuture` result.
  - `Timeout` - the maximum time the operation can stay in the queue on the server.

    - The operation will return `false` if the semaphore could not be acquired within `Timeout` after being added to the queue.
    - When `Timeout` is set to 0, the operation effectively works like `TryAcquire`, i.e., the semaphore will either be acquired atomically and the operation returns `true`, or the operation returns `false` without using queues.
  - `Ephemeral` - if `true`, the name is an ephemeral semaphore; such semaphores are automatically created on the first `Acquire` and automatically deleted with the last `Release`.
  - `Shared()` - an alias for setting `Count = 1`, acquiring the semaphore in shared mode.
  - `Exclusive()` - an alias for setting `Count = max`, acquiring the semaphore in exclusive mode (for semaphores created with limit `Max<ui64>()`).

- Go

  ```go
  lease, err := session.AcquireSemaphore(ctx,
      "my-semaphore",  // semaphore name
      5,              // value to increase semaphore by
  )
  ```


  To cancel waiting for semaphore acquisition, simply cancel the context `ctx` passed to the method.

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
        # working with resource
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
        # working with resource
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

  Acquisition is performed via `acquireSemaphore` (a full example is in the "Creating a semaphore" section). The method takes the semaphore name, the number of tokens `count`, optional operation data, and a queue wait timeout [java.time.Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html). Returns `CompletableFuture<Result<SemaphoreLease>>` (see [Result](https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/Result.java) and [SemaphoreLease](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java)). If a semaphore with the specified name does not exist, the operation will complete with an exception.

  For **ephemeral** semaphores, use `acquireEphemeralSemaphore` (the `exclusive` flag sets the acquisition mode); such semaphores are created on first acquisition and deleted after the last release.

  At any given time, a session can hold **only one** semaphore; repeated calls for the same name **replace** the previous operation (for example, to reduce `count` or change the timeout).

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

The acquired value of a captured semaphore can be decreased (but not increased) by calling the `AcquireSemaphore` method again with a smaller value.

### Updating semaphore data {#update-semaphore}

Using the `UpdateSemaphore` method, you can update (replace) the semaphore data that was attached when it was created.

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

  Updating data — the `updateSemaphore` method (step 4 in the example in the "Creating a semaphore" section). The call does not require capturing the semaphore and does not lead to it.

- Rust

  ```rust
  session
      .update_semaphore("my-semaphore", b"updated-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

This call does not require capturing the semaphore and does not lead to it. If you need only one specific client to update the data, this must be explicitly ensured, for example, by capturing the semaphore, updating the data, and releasing the semaphore back.

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


  When getting information about a semaphore, you can optionally pass a `TDescribeSemaphoreSettings` structure with the following settings:

  - `OnChanged` - called once after data changes on the server. With the `bool` parameter, if `true` - the call occurred due to some changes, if `false` - it is a false call and you need to repeat `DescribeSemaphore` to restore the subscription.
  - `WatchData` - call `OnChanged` in case of semaphore data changes.
  - `WatchOwners` - call `OnChanged` in case of semaphore owner changes.
  - `IncludeOwners` - return the list of owners in the results.
  - `IncludeWaiters` - return the list of waiters in the results.

  The result of the call is a structure with the following fields:

  - `Name` - semaphore name.
  - `Data` - semaphore data.
  - `Count` - current semaphore value.
  - `Limit` - maximum number of tokens specified when creating the semaphore.
  - `Owners` - list of semaphore owners.
  - `Waiters` - list of waiters in the semaphore queue.
  - `Ephemeral` - whether the semaphore is ephemeral.

  The `Owners` and `Waiters` fields in the result are a list of structures with the following fields:

  - `OrderId` - sequence number of the capture operation on the semaphore. Can be used for identification, for example if `OrderId` has changed, it means the session performed `ReleaseSemaphore` and a new `AcquireSemaphore`.
  - `SessionId` - session ID that performed this `AcquireSemaphore`.
  - `Timeout` - timeout with which `AcquireSemaphore` was called for queue operations.
  - `Count` - value requested in `AcquireSemaphore`.
  - `Data` - data specified in `AcquireSemaphore`.

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

  Reading the semaphore state — the `describeSemaphore` method (step 5 in the example in the "Creating a semaphore" section). It takes the semaphore name and the [DescribeSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/DescribeSemaphoreMode.java): only data, with the list of owners, with the list of waiters, or both lists.

  For the elements of the owner and waiter lists (`getOwnersList`, `getWaitersList`), the session ID, timeout, requested `count`, operation data, and `orderId` are available (see the nested type `SemaphoreDescription.Session` in the source code).

  To subscribe to changes, use `watchSemaphore` with the same description mode and [WatchSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/WatchSemaphoreMode.java) (data, owners, or both). The [SemaphoreWatcher](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreWatcher.java) object contains a snapshot of `SemaphoreDescription` and `getChangedFuture()` — `CompletableFuture<Result<SemaphoreChangedEvent>>` (see [SemaphoreChangedEvent](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreChangedEvent.java), fields `isDataChanged`, `isOwnersChanged`). The Future completes on the next event; after notification, call `watchSemaphore` again to continue watching (see [tests](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/test/java/tech/ydb/coordination/CoordinationServiceTest.java)).

- Rust

  [`describe_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore) by default requests owners and waiters. The set of flags can be set via [`DescribeOptions`](https://docs.rs/ydb/latest/ydb/struct.DescribeOptions.html) and [`describe_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore_with_params). For subscribing to changes, see [`WatchOptions`](https://docs.rs/ydb/latest/ydb/struct.WatchOptions.html) in the crate documentation.


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

  To release a semaphore acquired in a session, call the `Release` method on the `Lease` object.


  ```go
  err := lease.Release()
  ```

- Python

  In the Python SDK, a semaphore is released by calling the `release()` method on the semaphore object. When using a context manager (`with` or `async with`), the release happens automatically when exiting the block.

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # working with resource
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # working with resource
        await semaphore.release()
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  To release a semaphore acquired in a session, call the `Release` method on the `Lease` object. If the semaphore was acquired using a using construct, it will be automatically released when exiting the scope.


  ```javascript
  await lease.release();
  ```

- Java

  Release — via [SemaphoreLease.release()](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java) (step 6 in the example in the "Creating a semaphore" section). The method is asynchronous and returns `CompletableFuture<Status>`.

- Rust

  Call [`Lease::release`](https://docs.rs/ydb/latest/ydb/struct.Lease.html#method.release) or simply drop the `Lease` — when the value is destroyed, a release is also sent to the server.


  ```rust
  let lease = session.acquire_semaphore("my-semaphore", 1).await?;
  // …
  lease.release();
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Important features

The `AcquireSemaphore` and `ReleaseSemaphore` operations are idempotent. If `AcquireSemaphore` was called on a semaphore, subsequent calls to `AcquireSemaphore` only change the acquire parameters. For example, calling `AcquireSemaphore` with `count=10` may add an operation to the queue. Before or after a successful acquire, you can call `AcquireSemaphore` with `count=9` again, reducing the number of acquired units; the new operation will replace the old one (which will complete with code `ABORTED` if it has not yet completed successfully). The position in the queue does not change despite replacing one `AcquireSemaphore` operation with another.

The `AcquireSemaphore` and `ReleaseSemaphore` operations return `bool`, indicating whether the operation changed the semaphore state. For example, `AcquireSemaphore` returns `false` if the semaphore acquisition failed within `Timeout` time because it was acquired by another. The `ReleaseSemaphore` operation may return `false` if the semaphore is not acquired in the current session.

The `AcquireSemaphore` operation in the queue can be completed early by calling `ReleaseSemaphore`. Regardless of the number of `AcquireSemaphore` calls for a specific semaphore in one session, release occurs with a single `ReleaseSemaphore` call, that is, the `AcquireSemaphore` and `ReleaseSemaphore` operations cannot be used as an analog of `Acquire` or `Release` on a recursive mutex.

The `DescribeSemaphore` operation with the `WatchData` or `WatchOwners` flags creates a subscription to semaphore changes. Any older subscription to the same semaphore in the session is canceled, causing `OnChanged(false)`. It is recommended to ignore `OnChanged` from previous `DescribeSemaphore` calls if a new replacement call is being made, for example, by remembering the current call ID.

The `OnChanged(false)` call can occur not only due to cancellation by a new `DescribeSemaphore`, but also for other reasons, such as a temporary connection break between the gRPC client and server, a temporary connection break between the gRPC server and the current service leader, or a change of the service leader, i.e., at the slightest suspicion that a notification might have been lost. To restore the subscription, the client code must make a new `DescribeSemaphore` call, correctly handling the situation where the result of the new call may be different (for example, if the notification was indeed lost).

## Examples

* [Distributed lock](../../recipes/ydb-sdk/distributed-lock.md)
* [Leader election](../../recipes/ydb-sdk/leader-election.md)
* [Service discovery](../../recipes/ydb-sdk/service-discovery.md)
* [Configuration publishing](../../recipes/ydb-sdk/config-publication.md)
