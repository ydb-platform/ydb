# Working with coordination nodes

This article describes how to use {{ ydb-short-name }} SDK to coordinate multiple instances of a client application by using [coordination nodes](../../concepts/datamodel/coordination-node.md) and the semaphores they contain.

## Creating a coordination node

Coordination nodes are created in {{ ydb-short-name }} databases in the same namespace as other schema objects, such as [tables](../../concepts/datamodel/table.md) and [topics](../../concepts/datamodel/topic.md).

{% list tabs %}

- Go

    ```go
    err := db.Coordination().CreateNode(ctx,
        "/path/to/mynode",
    )
    ```

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

- Java

  ```java
  CoordinationClient client = CoordinationClient.newClient(transport);
  ```


  A node is created by calling `createNode` with the full path to the node in the database. The database path prefix can be taken from `client.getDatabase()`.

  If necessary, set the node configuration via [NodeConfig](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/NodeConfig.java) using the chain `NodeConfig.create().with…`. Available parameters: periods `SelfCheckPeriod` and `SessionGracePeriod`, read consistency and session connection modes (`readConsistencyMode`, `attachConsistencyMode`), rate limiter counter mode (`rateLimiterCountersMode`). Default values match the description for C++ (see above). The resulting `NodeConfig` is passed to `CoordinationNodeSettings`.


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


  Additionally available are `alterNode` (configuration change), `dropNode` (node deletion), and `describeNode` (reading the current configuration).

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

- JavaScript

  ```javascript
  import { CoordinationClient } from "@ydbjs/coordination";

  let client = new CoordinationClient(driver);
  await client.createNode("/path/to/mynode", {});
  ```

- Rust

  The coordination client is returned from [`Client::coordination_client`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.coordination_client). The node is created via [`CoordinationClient::create_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_node) with a path and [`NodeConfig`](https://docs.rs/ydb/latest/ydb/struct.NodeConfig.html) (via [`NodeConfigBuilder`](https://docs.rs/ydb/latest/ydb/struct.NodeConfigBuilder.html)). Also available are [`alter_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.alter_node), [`drop_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.drop_node), [`describe_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.describe_node). A complete example is [`mutex.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/mutex.rs).


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

- Go

    ```go
    session, err := db.Coordination().CreateSession(ctx,
        "/path/to/mynode", // Coordination Node name in the database
    )
    ```

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


  Typical scenario: after a successful `connect()` you perform semaphore operations, then close the session via `close()` (conveniently — try-with-resources). While the session is active, the SDK automatically retries the connection on network failures according to the settings.

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

- JavaScript

  ```javascript
  import { CoordinationClient } from "@ydbjs/coordination";

  let client = new CoordinationClient(driver);
  await using session = await client.createSession("/path/to/mynode", {}, signal);
  ```

- Rust

  The session is created by [`CoordinationClient::create_session`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_session) with a path to the node and [`SessionOptions`](https://docs.rs/ydb/latest/ydb/struct.SessionOptions.html) ([`SessionOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.SessionOptionsBuilder.html): timeout, description, etc.). The stream to the node is started inside the session constructor; there is no separate `connect` call, as in Java.


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

- Go

  In the Go SDK, a session context `session.Context()` is used to track such situations; it ends together with the session. The SDK independently handles transport-level errors and restores the connection to the service, attempting to restore the session when possible. Thus, you only need to monitor the session context to react promptly to its loss.

- C++

  In the C++ SDK, the established session in the background maintains and automatically restores the connection to the {{ ydb-short-name }} cluster.

- Python

  In the Python SDK, the session automatically restores the connection to the {{ ydb-short-name }} cluster on failures. It is recommended to use a context manager (`with` or `async with`) to ensure the session is closed when exiting the block. When working with semaphores via a context manager (`with session.semaphore(name)` or `async with session.semaphore(name)`), the semaphore is automatically released upon exiting the block, and the session is closed when the context ends.

- JavaScript

  In the JS SDK, a signal `session.signal` is used to track such situations; it is aborted together with the session. The SDK independently handles transport-level errors and restores the connection to the service, attempting to restore the session when possible. Thus, you only need to monitor the session signal to avoid performing actions when the session has been closed or expired.

  The JavaScript SDK also provides a method to obtain a new session when the old one is lost, and this approach is recommended for long-term use `for await (session of client.openSession()) { session.signal }`.

- Java

  Close the session (`close()`) when your scenario has finished: this explicitly releases the connection to the node. While the session remains open, the SDK automatically retries the connection on network failures according to `CoordinationSessionSettings`. Hold a semaphore only for the duration of solving the user task and release it via `SemaphoreLease.release()` when the resource is no longer needed.

- Rust

  In [`CoordinationSession`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html), call [`alive`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.alive): it returns [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html), which is canceled when the session ends (similar to context tracking in Go). When releasing [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html) or upon `Drop` of the session, the semaphore release is sent to the server in the background.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Working with semaphores {#semaphore}

### Creating a semaphore {#create-semaphore}

When creating a semaphore you can specify its limit. The limit defines the maximum value to which it can be increased. Calls that try to increase the semaphore value beyond this limit will wait until their increase requests can be fulfilled so that the semaphore value does not exceed its limit.

{% list tabs %}

- Go

    ```go
    err := session.CreateSemaphore(ctx,
        "my-semaphore", // semaphore name
        10              // semaphore limit
    )
    ```

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

- Python

  In the Python SDK the semaphore is created implicitly on the first `acquire()` call in the `session.semaphore(name, limit)` method. The limit is specified when creating the semaphore object.

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # the semaphore will be created on the first acquire() with limit 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # the semaphore will be created on the first acquire() with limit 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  {% endlist %}

- JavaScript

  ```javascript
  const sem = session.semaphore("connections");
  await sem.create({
    limit: 10,
    data: new Uint8Array(),
  });
  ```

- Java

  The semaphore is created explicitly by the `createSemaphore` method of the connected session. You can pass user binary data stored with the semaphore (`byte[] data`); the method variant without the `data` parameter is equivalent to passing `null`. If a semaphore with this name already exists, the operation completes with the “already exists” status.


  ```java
  session.createSemaphore("my-semaphore", 10, new byte[] {0x00, 0x12})
      .join()
      .expectSuccess("create semaphore failed");
  ```

- Rust

  [`CoordinationSession::create_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.create_semaphore) accepts a name, a limit, and arbitrary bytes `data` stored with the semaphore.


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

To acquire a semaphore, the client must call the `AcquireSemaphore` method and wait for the special `Lease` object. This object serves as confirmation that the semaphore value was successfully increased and can be considered as such until the semaphore is explicitly released or the session in which the confirmation was obtained ends.

{% list tabs %}

- Go

    ```go
    lease, err := session.AcquireSemaphore(ctx,
        "my-semaphore",  // semaphore name
        5,              // value to increase semaphore by
    )
    ```


  To cancel waiting to acquire the semaphore, simply cancel the context `ctx` passed to the method.

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

- JavaScript

  ```javascript
  {
    await using lease = await sem.acquire({ count: 1, data: new Uint8Array() });
    await doWork(lease.signal);
  } // lease.release() called automatically
  ```

- Java

  Acquisition is performed via `acquireSemaphore` (full example – see the “Creating a semaphore” section). The method takes the semaphore name, the number of tokens `count`, optional operation data, and a queue wait timeout [java.time.Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html). It returns `CompletableFuture<Result<SemaphoreLease>>` (see [Result](https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/Result.java) and [SemaphoreLease](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java)). If a semaphore with the specified name does not exist, the operation ends with an exception.


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

Data update — the method `UpdateSemaphore` (step 4 in the “Creating a semaphore” example). The call does not require acquiring the semaphore and does not result in it.

{% list tabs %}

- Go

    ```go
    err := session.UpdateSemaphore(
        "my-semaphore",                                                          // semaphore name
        options.WithUpdateData([]byte("updated-data")),   // new semaphore data
    )
    ```

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

This call does not require acquiring the semaphore and does not result in it. If you need the data to be updated by only a single client, you must ensure this explicitly, for example by acquiring the semaphore, updating the data, and releasing the semaphore back.

### Getting semaphore data {#describe-semaphore}

{% list tabs %}

- Go

    ```go
    description, err := session.DescribeSemaphore(
        "my-semaphore"                                // semaphore name
        options.WithDescribeOwners(true), // to get list of owners
        options.WithDescribeWaiters(true), // to get list of waiters
    )
    ```

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


  ```java
  import tech.ydb.coordination.description.SemaphoreDescription;
  import tech.ydb.coordination.settings.DescribeSemaphoreMode;

  SemaphoreDescription description = session
      .describeSemaphore("my-semaphore", DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS)
      .join()
      .getValue();
  ```


  Elements of the owners and waiters lists (`getOwnersList`, `getWaitersList`) provide the session identifier, timeout, requested `count`, operation data, and `orderId` (see the nested type `SemaphoreDescription.Session` in the source).

  To subscribe to changes, use `watchSemaphore` with the same description mode and [WatchSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/WatchSemaphoreMode.java) (data, owners, or both). The [SemaphoreWatcher](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreWatcher.java) object contains a snapshot of `SemaphoreDescription` and `getChangedFuture()` — `CompletableFuture<Result<SemaphoreChangedEvent>>` (see [SemaphoreChangedEvent](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreChangedEvent.java), fields `isDataChanged`, `isOwnersChanged`). The Future completes on the next event; after notification, call `watchSemaphore` again to continue watching (see [tests](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/test/java/tech/ydb/coordination/CoordinationServiceTest.java)).

- Rust

  By default, [`describe_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore) requests owners and waiters. You can specify the set of flags via [`DescribeOptions`](https://docs.rs/ydb/latest/ydb/struct.DescribeOptions.html) and [`describe_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore_with_params). For subscribing to changes, see [`WatchOptions`](https://docs.rs/ydb/latest/ydb/struct.WatchOptions.html) in the crate documentation.


  ```rust
  let description = session.describe_semaphore("my-semaphore").await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Releasing a semaphore {#release-semaphore}

{% list tabs %}

- Go

  To release a semaphore acquired in a session, you need to call the `Release` method on the `Lease` object.


    ```go
    err := lease.Release()
    ```

- C++

    ```cpp
    session
        .ReleaseSemaphore(
            "my-semaphore"  // semaphore name
        )
        .ExtractValueSync()
        .ExtractResult();
    ```

- Python

  In the Python SDK, the semaphore is released by the `release()` method of the semaphore object. When using a context manager (`with` or `async with`), release happens automatically when exiting the block.

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

- JavaScript

  To release a semaphore acquired in a session, call the `Release` method on the `Lease` object. If the semaphore was acquired using the using construct, it will be released automatically when the scope is exited.


  ```javascript
  await lease.release();
  ```

- Java

  Release is performed via [SemaphoreLease.release()](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java) (step 6 in the “Creating a semaphore” example). The method is asynchronous and returns `CompletableFuture<Status>`.


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
