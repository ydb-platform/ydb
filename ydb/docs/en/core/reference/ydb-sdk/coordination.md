# Working with coordination nodes

This article describes how to use the {{ ydb-short-name }} SDK to coordinate the work of multiple client application instances using [coordination nodes](../../concepts/datamodel/coordination-node.md) and the semaphores contained in them.

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

  - `ReadConsistencyMode` - by default `RELAXED`, which allows reading a value that is not the most recent in case of a leader change. Optionally, you can enable `STRICT` read mode, in which all reads go through the consensus algorithm and guarantee returning the most recent value, but become significantly more expensive.
  - `AttachConsistencyMode` - by default `STRICT`, which means mandatory use of the consensus algorithm when restoring a session. Optionally, you can enable `RELAXED` session recovery mode in case of failures, which disables this requirement. The relaxed mode may be needed with a very large number of clients, allowing session recovery without going through consensus, which does not affect overall correctness, but may exacerbate reading a value that is not the most recent during a leader change, as well as session obsolescence in case of problems.
  - `SelfCheckPeriod` (default 1 second) - the frequency with which the service performs its own liveness checks. It is not recommended to change except in special cases.

    - The larger the specified value, the lower the load on the server, but the longer the possible delay between a leader change and how quickly the service itself learns about it.
    - The smaller the specified value, the higher the load on the server and the greater the responsiveness in detecting problems, but false positives may occur when the service erroneously detects problems.
  - `SessionGracePeriod` (default 10 seconds) - the period during which a new leader does not close open sessions, extending them.

    - The smaller the value, the smaller the window when sessions from non-existent clients that did not have time to report their disappearance during a leader change will hold semaphores and interfere with other clients.
    - The smaller the value, the higher the probability of false positives, when a live leader may terminate its work to be on the safe side, because it will not be sure that this period has not ended for the new leader.
    - Must be strictly greater than `SelfCheckPeriod`.

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


  A node is created by calling `createNode` with the full path to the node in the database. The database path prefix can be taken from `client.getDatabase()`.

  If necessary, set the node configuration through [NodeConfig](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/NodeConfig.java) using the chain `NodeConfig.create().with…`. Available parameters: periods `SelfCheckPeriod` and `SessionGracePeriod`, read consistency and session connection modes (`readConsistencyMode`, `attachConsistencyMode`), rate limiter counter mode (`rateLimiterCountersMode`). Default values match the description for C++ (see above). The resulting `NodeConfig` is passed to `CoordinationNodeSettings`.


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


  Additionally, `alterNode` (configuration change), `dropNode` (node deletion), and `describeNode` (reading current configuration) are available.

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

  Coordination client is returned from [`Client::coordination_client`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.coordination_client). A node is created via [`CoordinationClient::create_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_node) with a path and [`NodeConfig`](https://docs.rs/ydb/latest/ydb/struct.NodeConfig.html) (via [`NodeConfigBuilder`](https://docs.rs/ydb/latest/ydb/struct.NodeConfigBuilder.html)). Also available are [`alter_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.alter_node), [`drop_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.drop_node), [`describe_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.describe_node). A complete example is [`mutex.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/mutex.rs).


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
  - `OnStateChanged` - called on important changes during the session lifecycle, passing the corresponding state:

    - `ATTACHED` - the session is connected and operating normally.
    - `DETACHED` - the session temporarily lost connection to the service but can still be restored.
    - `EXPIRED` - the session lost connection to the service and cannot be restored.
  - `OnStopped` - called when the session stops trying to reconnect to the service, which can be useful for establishing a new connection.
  - `Timeout` - the maximum timeout during which the session can be restored after losing connection to the service.

- Go

  ```go
  session, err := db.Coordination().CreateSession(ctx,
      "/path/to/mynode", // Coordination Node name in the database
  )
  ```

- Java

  Session (see [CoordinationSession](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/CoordinationSession.java)) is created via `createSession`; to establish a bidirectional gRPC stream with a node, you need to call `connect()` (asynchronously, returns `CompletableFuture<Status>`). Retry parameters and connection timeout are set in [CoordinationSessionSettings](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/CoordinationSessionSettings.java) (`withConnectTimeout`, `withRetryPolicy`, `withExecutor`).


  ```java
  import tech.ydb.coordination.CoordinationSession;
  import tech.ydb.coordination.settings.CoordinationSessionSettings;

  CoordinationSession session = client.createSession(
      "/path/to/mynode",
      CoordinationSessionSettings.newBuilder().build()
  );

  session.connect().join().expectSuccess("connect failed");
  ```


  A typical scenario: after a successful `connect()`, you perform semaphore operations for your task (locking, leadership, etc.), then close the session. It is convenient to use try-with-resources so that when the work is complete, `close()` is called and the node thread is properly stopped.

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

  A session is created by [`CoordinationClient::create_session`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_session) with the path to the node and [`SessionOptions`](https://docs.rs/ydb/latest/ydb/struct.SessionOptions.html) ([`SessionOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.SessionOptionsBuilder.html): timeout, description, etc.). The node thread is started inside the session constructor; there is no separate `connect` call as in Java.


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

### Session Termination Control {#session-control}

The client application must monitor the session state, because it can rely on the state of acquired semaphores only while the session is active. When the session terminates by the client or server, the client can no longer be sure that other clients in the cluster have not acquired its semaphores and changed their state.

{% list tabs %}

- C++

  In the C++ SDK, an established session maintains and automatically restores connection to the {{ ydb-short-name }} cluster in the background.

- Go

  In the Go SDK, the session context `session.Context()` is used to track such situations; it terminates together with the session. The SDK independently handles transport layer errors and restores the connection to the service, attempting to restore the session if possible. Thus, the client only needs to monitor the session context to react in a timely manner to its loss.

- Python

  In the Python SDK, the session automatically restores connection to the {{ ydb-short-name }} cluster on failures. It is recommended to use a context manager (`with` or `async with`) to guarantee session closure when exiting the block. When working with semaphores via a context manager (`with session.semaphore(name)` or `async with session.semaphore(name)`), the semaphore is automatically released when exiting the block, and the session is released when the context is closed.

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  In the JS SDK, the signal `session.signal` is used to track such situations; it is interrupted together with the session. The SDK independently handles transport layer errors and restores the connection to the service, attempting to restore the session if possible. Thus, the client only needs to monitor the session signal to avoid performing actions when the session has been closed or expired.

  Also, the JavaScript SDK has a method to obtain a new session when the old one is lost, and this approach is recommended for long-term use of `for await (session of client.openSession()) { session.signal }`.

- Java

  Terminate the session (`close()`) when your scenario is complete: this explicitly releases the connection to the node. While the session is not closed, the SDK retries the connection on network failures according to `CoordinationSessionSettings`. Hold the semaphore only for the duration of the user task and release it via `SemaphoreLease.release()` when the resource is no longer needed.

- Rust

  Call [`alive`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.alive) on [`CoordinationSession`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html): it returns [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) — when the session ends, it is cancelled (similar to context tracking in Go). When [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html) is released or when the session `Drop`, the semaphore release is sent to the server in the background.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Working with semaphores {#semaphore}

### Creating a semaphore {#create-semaphore}

When creating a semaphore, you can specify its limit. The limit determines the maximum value to which it can be increased. Calls that try to increase the semaphore value above this limit will wait until their increase requests can be fulfilled, so that the semaphore value does not exceed its limit.

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

  A semaphore is created explicitly by the `createSemaphore` method on a connected session. You can pass custom binary data stored with the semaphore (`byte[] data`); the method variant without the `data` parameter is equivalent to passing `null`. If a semaphore with that name already exists, the operation completes with a 'already exists' status.


  ```java
  session.createSemaphore("my-semaphore", 10, new byte[] {0x00, 0x12})
      .join()
      .expectSuccess("create semaphore failed");
  ```

- Rust

  [`CoordinationSession::create_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.create_semaphore) takes a name, limit, and arbitrary bytes `data` stored with the semaphore.


  ```rust
  session.create_semaphore("my-semaphore", 10, vec![]).await?;

  // or with user data stored with the semaphore:
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

  - `Count` - the value by which the semaphore is increased when acquired.
  - `Data` - additional data that can be placed in the semaphore.
  - `OnAccepted` - called when the operation is queued (for example, if the semaphore could not be acquired immediately).

    - It will not be called if the semaphore is acquired immediately.
    - It is important to note that the call may occur concurrently with the `TFuture` result.
  - `Timeout` - the maximum time the operation can stay in the queue on the server.

    - The operation will return `false` if it fails to acquire the semaphore within `Timeout` after being added to the queue.
    - When `Timeout` is set to 0, the operation semantically works like `TryAcquire`, i.e., the semaphore will either be acquired atomically and the operation returns `true`, or the operation returns `false` without using queues.
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

  Acquisition is performed via `acquireSemaphore` with the semaphore name, number of tokens `count`, optional operation data, and queue wait timeout [java.time.Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html). The method returns `CompletableFuture<Result<SemaphoreLease>>` (see [Result](https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/Result.java) and [SemaphoreLease](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java)). If a semaphore with the specified name does not exist, the operation completes with an exception (see the method's javadoc).


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


  For **ephemeral** semaphores, use `acquireEphemeralSemaphore` (the `exclusive` flag sets the acquisition mode); such semaphores are created on the first acquire and deleted after the last release.

  The API documentation states: at any one time, a session can hold **only one** semaphore; repeated calls for the same name **replace** the previous operation (for example, to decrease `count` or change the timeout).

- Rust

  [`acquire_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore) returns [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html). Queue wait timeout, ephemerality, and data operations are set via [`AcquireOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.AcquireOptionsBuilder.html) and [`acquire_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore_with_params).


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

This call does not require capturing the semaphore and does not lead to it. If you need only one specific client to update the data, you must explicitly ensure this, for example, by capturing the semaphore, updating the data, and releasing the semaphore back.

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

  - `OnChanged` - called once after data changes on the server. With the `bool` parameter, if `true` - then the call occurred due to some changes, if `false` - then it is a false call and you need to repeat `DescribeSemaphore` to restore the subscription.
  - `WatchData` - call `OnChanged` in case of semaphore data changes.
  - `WatchOwners` - call `OnChanged` in case of semaphore owner changes.
  - `IncludeOwners` - return the list of owners in the results.
  - `IncludeWaiters` - return the list of waiters in the results.

  The call result is a structure with the following fields:

  - `Name` - semaphore name.
  - `Data` - semaphore data.
  - `Count` - current semaphore value.
  - `Limit` - maximum number of tokens specified when creating the semaphore.
  - `Owners` - list of semaphore owners.
  - `Waiters` - list of waiters in the semaphore queue.
  - `Ephemeral` - whether the semaphore is ephemeral.

  The `Owners` and `Waiters` fields in the result are a list of structures with the following fields:

  - `OrderId` - sequence number of the capture operation on the semaphore. Can be used for identification, for example if `OrderId` changed, it means the session performed `ReleaseSemaphore` and a new `AcquireSemaphore`.
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

  The `describeSemaphore` method takes the semaphore name and the [DescribeSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/DescribeSemaphoreMode.java): only data, with a list of owners, with a list of waiters, or both lists.


  ```java
  import tech.ydb.coordination.description.SemaphoreDescription;
  import tech.ydb.coordination.settings.DescribeSemaphoreMode;

  SemaphoreDescription description = session
      .describeSemaphore("my-semaphore", DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS)
      .join()
      .getValue();
  ```


  The elements of the owner and waiter lists (`getOwnersList`, `getWaitersList`) have available session ID, timeout, requested `count`, operation data, and `orderId` (see the nested type `SemaphoreDescription.Session` in the source code).

  To subscribe to changes, use `watchSemaphore` with the same description mode and [WatchSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/WatchSemaphoreMode.java) (data, owners, or both). The [SemaphoreWatcher](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreWatcher.java) object contains a snapshot of `SemaphoreDescription` and `getChangedFuture()` — `CompletableFuture<Result<SemaphoreChangedEvent>>` (see [SemaphoreChangedEvent](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreChangedEvent.java), fields `isDataChanged`, `isOwnersChanged`). The Future completes on the next event; after notification, call `watchSemaphore` again to continue watching (see [tests](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/test/java/tech/ydb/coordination/CoordinationServiceTest.java)).

- Rust

  [`describe_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore) by default requests owners and waiters. You can set the flags via [`DescribeOptions`](https://docs.rs/ydb/latest/ydb/struct.DescribeOptions.html) and [`describe_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore_with_params). For subscribing to changes, see [`WatchOptions`](https://docs.rs/ydb/latest/ydb/struct.WatchOptions.html) in the crate documentation.


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

  In the Python SDK, a semaphore is released by calling the `release()` method on the semaphore object. When using a context manager (`with` or `async with`), the release happens automatically upon exiting the block.

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

  To release a semaphore acquired in a session, call the `Release` method on the `Lease` object. If the semaphore was acquired using a using construct, it will be released automatically when the scope is exited.


  ```javascript
  await lease.release();
  ```

- Java

  Release via [SemaphoreLease.release()](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java) (asynchronously, `CompletableFuture<Status>`).


  ```java
  lease.release().join().expectSuccess("release failed");
  ```

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

The `AcquireSemaphore` and `ReleaseSemaphore` operations are idempotent. If `AcquireSemaphore` was called on the semaphore, repeated calls of `AcquireSemaphore` change only the acquisition parameters. For example, a call of `AcquireSemaphore` with `count=10` can add an operation to the queue. Before or after a successful acquisition, you can call `AcquireSemaphore` with `count=9` again, reducing the number of acquired units; the new operation will replace the old one (which will complete with code `ABORTED` if it has not yet completed successfully). The position in the queue does not change despite the replacement of one operation `AcquireSemaphore` with another.

The `AcquireSemaphore` and `ReleaseSemaphore` operations return `bool`, indicating whether the operation changed the semaphore state. For example, `AcquireSemaphore` will return `false` if the semaphore acquisition failed within `Timeout` time because it was acquired by another. The `ReleaseSemaphore` operation may return `false` if the semaphore is not acquired in the current session.

The `AcquireSemaphore` operation that is in the queue can be completed early by calling `ReleaseSemaphore`. Regardless of the number of `AcquireSemaphore` calls for a specific semaphore in one session, release occurs with a single `ReleaseSemaphore` call, that is, the `AcquireSemaphore` and `ReleaseSemaphore` operations cannot be used as an analog of `Acquire` or `Release` on a recursive mutex.

The `DescribeSemaphore` operation with the `WatchData` or `WatchOwners` flags creates a subscription to semaphore changes. Any older subscription to the same semaphore in the session is canceled, causing `OnChanged(false)`. It is recommended to ignore `OnChanged` from previous `DescribeSemaphore` calls if a new replacement call is being made, for example, by remembering the current call id.

The `OnChanged(false)` call can occur not only due to cancellation by a new `DescribeSemaphore`, but also for other reasons, for example, during a temporary connection break between the gRPC client and server, during a temporary connection break between the gRPC server and the current service leader, or when the service leader changes, that is, at the slightest suspicion that a notification might have been lost. To restore the subscription, the client code must make a new `DescribeSemaphore` call, correctly handling the situation where the result of the new call may be different (for example, if the notification was indeed lost).

## Examples

* [Distributed lock](../../recipes/ydb-sdk/distributed-lock.md)
* [Leader election](../../recipes/ydb-sdk/leader-election.md)
* [Service discovery](../../recipes/ydb-sdk/service-discovery.md)
* [Configuration publishing](../../recipes/ydb-sdk/config-publication.md)
