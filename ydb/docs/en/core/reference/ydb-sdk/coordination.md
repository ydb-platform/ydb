# Working with coordination nodes

This article describes how to use the {{ ydb-short-name }} SDK to coordinate the work of multiple client application instances using [coordination nodes](../../concepts/datamodel/coordination-node.md) and their semaphores.

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

   When creating a node, you can optionally specify `TNodeSettings` with the following settings:

   - `ReadConsistencyMode` - defaults to `RELAXED`, allowing the reading of potentially outdated values during leader transitions. You can optionally enable the `STRICT` mode, where all reads are processed through the consensus algorithm, ensuring the most recent value is returned, albeit at a higher cost.
   - `AttachConsistencyMode` - defaults to `STRICT`, requiring the consensus algorithm for session recovery. Optionally, the `RELAXED` mode can be enabled for session recovery during failures, bypassing this requirement. This mode may be necessary for a large number of clients, facilitating session recovery without consensus, which maintains overall correctness but may lead to outdated reads during leader transitions and session expiration in problematic scenarios.
   - `SelfCheckPeriod` (default 1 second) - the interval at which the service performs self-liveness checks. It is not recommended to change this setting except under special circumstances.

     - A larger value reduces server load but increases the delay in detecting leader changes and informing the service.
     - A smaller value increases server load and improves problem detection speed, but may result in false positives when the service incorrectly identifies issues.

   - `SessionGracePeriod` (default 10 seconds) - the duration during which a new leader refrains from closing existing open sessions, prolonging their validity.

     - A smaller value reduces the window during which sessions from non-existent clients, which failed to report their absence during leader changes, hold semaphores and block other clients.
     - A smaller value also increases the likelihood of false positives, where a living leader might terminate itself as a precaution, uncertain if this period has concluded on a potential new leader.
     - This value must be strictly greater than `SelfCheckPeriod`.

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

  Create a node by calling `createNode` with the full path to the node in the database. You can take the database path prefix from `client.getDatabase()`.

  When needed, configure the node via [NodeConfig](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/NodeConfig.java) using the `NodeConfig.create().with…` chain. Available parameters: `SelfCheckPeriod` and `SessionGracePeriod`, read and attach consistency modes (`readConsistencyMode`, `attachConsistencyMode`), and rate limiter counters mode (`rateLimiterCountersMode`). Defaults match the C++ description above. Pass the resulting `NodeConfig` to `CoordinationNodeSettings`.

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

  You can also use `alterNode` (change configuration), `dropNode` (delete the node), and `describeNode` (read the current configuration).

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

  The coordination client is returned by [`Client::coordination_client`](https://docs.rs/ydb/latest/ydb/struct.Client.html#method.coordination_client). A node is created via [`CoordinationClient::create_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_node) with a path and [`NodeConfig`](https://docs.rs/ydb/latest/ydb/struct.NodeConfig.html) (built via [`NodeConfigBuilder`](https://docs.rs/ydb/latest/ydb/struct.NodeConfigBuilder.html)). Also available: [`alter_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.alter_node), [`drop_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.drop_node), [`describe_node`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.describe_node). For a complete example, see [`mutex.rs`](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/mutex.rs).

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

To start working with coordination nodes, a client must establish a session within which it will perform all operations with the coordination node.

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

   - `Description` - a text description of the session, displayed in internal interfaces and can be useful for problem diagnosis.
   - `OnStateChanged` - called on significant changes during the session's life, passing the corresponding state:

     - `ATTACHED` - the session is connected and operating in normal mode.
     - `DETACHED` - the session temporarily lost connection with the service but can still be restored.
     - `EXPIRED` - the session lost connection with the service and cannot be restored.

   - `OnStopped` - called when the session stops attempting to restore the connection with the service, which can be useful for establishing a new connection.
   - `Timeout` - the maximum timeout during which the session can be restored after losing connection with the service.

- Go

    ```go
    session, err := db.Coordination().CreateSession(ctx,
        "/path/to/mynode", // Coordination Node name in the database
    )
    ```

- Java

  A session (see [CoordinationSession](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/CoordinationSession.java)) is created with `createSession`; call `connect()` to establish the bidirectional gRPC stream with the node (asynchronous, returns `CompletableFuture<Status>`). Retry behavior and connect timeout are configured in [CoordinationSessionSettings](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/CoordinationSessionSettings.java) (`withConnectTimeout`, `withRetryPolicy`, `withExecutor`).

  ```java
  import tech.ydb.coordination.CoordinationSession;
  import tech.ydb.coordination.settings.CoordinationSessionSettings;

  CoordinationSession session = client.createSession(
      "/path/to/mynode",
      CoordinationSessionSettings.newBuilder().build()
  );

  session.connect().join().expectSuccess("connect failed");
  ```

  Typical flow: after a successful `connect()`, run semaphore operations for your scenario (locking, leadership, and so on), then close the session. Using try-with-resources is convenient so that `close()` stops the stream to the node when you are done.

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # work with the session
        pass
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # work with the session
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

  A session is created by [`CoordinationClient::create_session`](https://docs.rs/ydb/latest/ydb/struct.CoordinationClient.html#method.create_session) with the node path and [`SessionOptions`](https://docs.rs/ydb/latest/ydb/struct.SessionOptions.html) ([`SessionOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.SessionOptionsBuilder.html): timeout, description, etc.). The stream to the node is established inside the session constructor; there is no separate `connect` call as in Java.

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

### Session control {#session-control}

It's important for the client application to monitor the session state, as it can only rely on the state of captured semaphores while the session is alive. When the session ends by client or server initiative, the client can no longer assume that other clients in the cluster haven't captured its semaphores and changed their state.

{% list tabs %}

- C++

  In the C++ SDK, an active session continuously maintains and automatically re-establishes the connection with the {{ ydb-short-name }} cluster in the background.

- Go

  In Go SDK, the session context `session.Context()` is used to track such situations, which terminates along with the session. The SDK can handle transport-level errors on its own and re-establish connection with the service, trying to restore the session if still possible. Thus, the client only needs to monitor the session context to react timely to its loss.

- Python

  In the Python SDK, the session automatically restores the connection to the {{ ydb-short-name }} cluster after failures. Use a context manager (`with` or `async with`) to ensure the session is closed when leaving the block. When working with semaphores through a context manager (`with session.semaphore(name)` or `async with session.semaphore(name)`), the semaphore is released automatically when leaving the block, and the session is closed when the context exits.

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  In the JavaScript SDK, you can use `session.signal` to monitor session lifetime: it is aborted when the session is closed or expires. The SDK handles transport-level errors and reconnects to the service, attempting to restore the session when possible. This means the client only needs to watch the session signal and avoid any operations once the session is closed or expired.

  For long-running applications, the JavaScript SDK provides a recommended pattern to automatically obtain a new session when the previous one is lost: `for await (session of client.openSession()) { session.signal }`

- Java

  Call `close()` when your scenario is finished; this explicitly releases the connection to the node. Until the session is closed, the SDK retries the connection on network failures according to `CoordinationSessionSettings`. Hold a semaphore only while solving your business task and release it with `SemaphoreLease.release()` when the resource is no longer needed.

- Rust

  On [`CoordinationSession`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html), call [`alive`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.alive): it returns a [`CancellationToken`](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html) that is canceled when the session ends (analogous to context tracking in Go). When a [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html) is released or the session is dropped, the semaphore release is sent to the server in the background.

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Working with semaphores {#semaphore}

### Creating a semaphore {#create-semaphore}

When creating a semaphore, you can specify its limit. The limit determines the maximum value to which it can be increased. Calls attempting to increase the semaphore value above this limit will wait until their increase requests can be fulfilled without exceeding the semaphore's limit.

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

    You can also pass a string that will be stored with the semaphore and returned when it's captured:

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

  In the Python SDK, a semaphore is created implicitly on the first `acquire()` call in `session.semaphore(name, limit)`. The limit is set when the semaphore object is created.

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        # semaphore is created on first acquire() with limit 10
        semaphore = session.semaphore("my-semaphore", 10)
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        # semaphore is created on first acquire() with limit 10
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

  Create a semaphore explicitly with `createSemaphore` on a connected session. You can pass custom binary data stored with the semaphore (`byte[] data`); the overload without `data` is equivalent to passing `null`. If a semaphore with that name already exists, the operation completes with an “already exists” status.

  ```java
  session.createSemaphore("my-semaphore", 10, new byte[] {0x00, 0x12})
      .join()
      .expectSuccess("create semaphore failed");
  ```

- Rust

  [`CoordinationSession::create_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.create_semaphore) takes a name, a limit, and arbitrary `data` bytes stored with the semaphore.

  ```rust
  session.create_semaphore("my-semaphore", 10, vec![]).await?;

  // or with custom data stored with the semaphore:
  session
      .create_semaphore("other-semaphore", 10, b"my-data".to_vec())
      .await?;
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

### Acquiring a semaphore {#acquire-semaphore}

To acquire a semaphore, the client must call the `AcquireSemaphore` method and wait for a special `Lease` object. This object represents confirmation that the semaphore value was successfully increased and can be considered as such until explicit release of such semaphore or termination of the session in which such confirmation was received.

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

    - `Count` - value by which the semaphore is increased upon acquisition.
    - `Data` - additional data that can be put into the semaphore.
    - `OnAccepted` - called when the operation is queued. For example, if the semaphore couldn't be acquired immediately.

      - Won't be called if the semaphore is acquired immediately.
      - It's important to consider that the call can happen in parallel with the `TFuture` result.

    - `Timeout` - maximum time during which the operation can stay in the queue on the server.

      - Operation will return `false` if the semaphore couldn't be acquired within `Timeout` after being added to the queue.
      - With `Timeout` set to 0, the operation works like `TryAcquire`, i.e., the semaphore will either be acquired atomically and the operation will return `true`, or the operation will return `false` without using queues.

    - `Ephemeral` - if `true`, then the name is an ephemeral semaphore. Such semaphores are automatically created at first `Acquire` and automatically deleted with the last `Release`.
    - `Shared()` - alias for setting `Count = 1`, acquiring semaphore in shared mode.
    - `Exclusive()` - alias for setting `Count = max`, acquiring semaphore in exclusive mode. (For semaphores created with limit `Max<ui64>()`.)

- Go

    ```go
    lease, err := session.AcquireSemaphore(ctx,
        "my-semaphore",  // semaphore name
        5,              // value to increase semaphore by
    )
    ```

    To cancel waiting for semaphore acquisition, it's sufficient to cancel the context `ctx` passed to the method.

- Python

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        with semaphore:
            # semaphore acquired for 1 unit (default)
            pass
        # or manually:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # work with the resource
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        async with semaphore:
            # semaphore acquired for 1 unit (default)
            pass
        # or manually:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # work with the resource
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

  Acquisition is done via `acquireSemaphore` with the semaphore name, token `count`, optional operation data, and queue wait timeout as [java.time.Duration](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html). The method returns `CompletableFuture<Result<SemaphoreLease>>` (see [Result](https://github.com/ydb-platform/ydb-java-sdk/blob/master/core/src/main/java/tech/ydb/core/Result.java) and [SemaphoreLease](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java)). If no semaphore exists with the given name, the operation fails with an exception (see the method javadoc).

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

  For **ephemeral** semaphores, use `acquireEphemeralSemaphore` (the `exclusive` flag sets the acquisition mode); such semaphores are created on first acquire and removed after the last release.

  The API documentation states that a session can hold **only one** semaphore at a time; repeated calls for the same name **replace** the previous operation (for example, to reduce `count` or change the timeout).

- Rust

  [`acquire_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore) returns a [`Lease`](https://docs.rs/ydb/latest/ydb/struct.Lease.html). The queue wait timeout, ephemerality, and operation data are set via [`AcquireOptionsBuilder`](https://docs.rs/ydb/latest/ydb/struct.AcquireOptionsBuilder.html) and [`acquire_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.acquire_semaphore_with_params).

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

The taken value of an acquired semaphore can be decreased (but not increased) by calling the `AcquireSemaphore` method again with a smaller value.

### Updating semaphore data {#update-semaphore}

Using the `UpdateSemaphore` method, you can update (replace) the semaphore data that was attached during its creation.

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
        "my-semaphore",                                   // semaphore name
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

This call doesn't require acquiring the semaphore and doesn't lead to it. If you need only one specific client to update the data, this must be explicitly ensured, for example, by acquiring the semaphore, updating the data, and releasing the semaphore back.

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

    - `OnChanged` - called once after data changes on the server (with a `bool` parameter, if `true` - the call occurred due to some changes, if `false` - it's a false call and you need to repeat `DescribeSemaphore` to restore the subscription).
    - `WatchData` - call `OnChanged` when semaphore data changes.
    - `WatchOwners` - call `OnChanged` when semaphore owners change.
    - `IncludeOwners` - return the list of owners in the results.
    - `IncludeWaiters` - return the list of waiters in the results.

    The call result is a structure with the following fields:

    - `Name` - semaphore name.
    - `Data` - semaphore data.
    - `Count` - the current value of the semaphore.
    - `Limit` - the limit specified when creating the semaphore.
    - `Owners` - list of semaphore owners.
    - `Waiters` - list of clients waiting in the semaphore queue.
    - `Ephemeral` - whether the semaphore is ephemeral.

    The `Owners` and `Waiters` fields in the result contain a list of structures with the following fields:

    - `OrderId` - sequence number of the acquire operation on the semaphore (can be used for identification, for example if `OrderId` changed, it means the session called `ReleaseSemaphore` and a new `AcquireSemaphore`).
    - `SessionId` - identifier of the session that made this `AcquireSemaphore` call.
    - `Timeout` - timeout value used in the `AcquireSemaphore` call for queued operations.
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

  The `describeSemaphore` method takes a semaphore name and a [DescribeSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/DescribeSemaphoreMode.java): data only, with owners list, with waiters list, or both.

  ```java
  import tech.ydb.coordination.description.SemaphoreDescription;
  import tech.ydb.coordination.settings.DescribeSemaphoreMode;

  SemaphoreDescription description = session
      .describeSemaphore("my-semaphore", DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS)
      .join()
      .getValue();
  ```

  Owner and waiter list entries (`getOwnersList`, `getWaitersList`) expose session id, timeout, requested `count`, operation data, and `orderId` (see the nested `SemaphoreDescription.Session` type in the sources).

  To subscribe to changes, use `watchSemaphore` with the same describe mode and [WatchSemaphoreMode](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/settings/WatchSemaphoreMode.java) (data, owners, or both). [SemaphoreWatcher](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreWatcher.java) holds a `SemaphoreDescription` snapshot and `getChangedFuture()` — `CompletableFuture<Result<SemaphoreChangedEvent>>` (see [SemaphoreChangedEvent](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/description/SemaphoreChangedEvent.java), fields `isDataChanged`, `isOwnersChanged`). The future completes on the next event; call `watchSemaphore` again to continue watching (see [tests](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/test/java/tech/ydb/coordination/CoordinationServiceTest.java)).

- Rust

  [`describe_semaphore`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore) requests owners and waiters by default. The set of flags can be configured via [`DescribeOptions`](https://docs.rs/ydb/latest/ydb/struct.DescribeOptions.html) and [`describe_semaphore_with_params`](https://docs.rs/ydb/latest/ydb/struct.CoordinationSession.html#method.describe_semaphore_with_params). For subscribing to changes, see [`WatchOptions`](https://docs.rs/ydb/latest/ydb/struct.WatchOptions.html) in the crate documentation.

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

  In the Python SDK, a semaphore is released with the `release()` method on the semaphore object. When using a context manager (`with` or `async with`), release happens automatically when leaving the block.

  {% list tabs %}
  - Native SDK

    ```python
    import ydb

    client = driver.coordination_client
    with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        semaphore.acquire(count=5)
        # work with the resource
        semaphore.release()
    ```

  - Native SDK (Asyncio)

    ```python
    import ydb

    client = driver.coordination_client
    async with client.session("/path/to/mynode") as session:
        semaphore = session.semaphore("my-semaphore", 10)
        await semaphore.acquire(count=5)
        # work with the resource
        await semaphore.release()
    ```

  {% endlist %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  To release a semaphore acquired within a session, call `release()` on the `Lease` object. If you use `await using`, the lease is released automatically when leaving the scope.

  ```javascript
  await lease.release();
  ```

- Java

  Release via [SemaphoreLease.release()](https://github.com/ydb-platform/ydb-java-sdk/blob/master/coordination/src/main/java/tech/ydb/coordination/SemaphoreLease.java) (asynchronous, `CompletableFuture<Status>`).

  ```java
  lease.release().join().expectSuccess("release failed");
  ```

- Rust

  Call [`Lease::release`](https://docs.rs/ydb/latest/ydb/struct.Lease.html#method.release) or simply drop ownership of the `Lease` — when the value is destroyed, the release is also sent to the server.

  ```rust
  let lease = session.acquire_semaphore("my-semaphore", 1).await?;
  // …
  lease.release();
  ```

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}

## Important implementation details

The `AcquireSemaphore` and `ReleaseSemaphore` operations are idempotent. When `AcquireSemaphore` is invoked on a semaphore, subsequent calls to `AcquireSemaphore` only adjust the acquisition parameters. For instance, if `AcquireSemaphore` is called with `count=10`, the operation might be queued. You can call `AcquireSemaphore` again with `count=9` before or after successful acquisition, reducing the number of acquired units. The new operation replaces the old one, which will complete with an `ABORTED` code if it hasn't completed successfully yet. The queue position remains unchanged despite replacing one `AcquireSemaphore` operation with another.

Both `AcquireSemaphore` and `ReleaseSemaphore` operations return a `bool` indicating whether the semaphore state was altered. For example, `AcquireSemaphore` might return `false` if the semaphore couldn't be acquired within the `Timeout` period because it was acquired by another entity. Similarly, `ReleaseSemaphore` might return `false` if the semaphore isn't acquired within the current session.

A queued `AcquireSemaphore` operation can be prematurely terminated by calling `ReleaseSemaphore`. Regardless of how many `AcquireSemaphore` calls are made for a specific semaphore within one session, a single `ReleaseSemaphore` call releases it. Thus, `AcquireSemaphore` and `ReleaseSemaphore` operations cannot function as `Acquire`/`Release` on a recursive mutex.

The `DescribeSemaphore` operation with `WatchData` or `WatchOwners` flags set establishes a subscription for semaphore changes. Any previous subscription to the same semaphore within the session is canceled, triggering `OnChanged(false)`. It is advisable to disregard `OnChanged` from earlier `DescribeSemaphore` calls if a new replacing call is made, for instance, by tracking a current call ID.

The `OnChanged(false)` call might occur not only due to cancellation by a new `DescribeSemaphore` but also for various reasons, such as temporary connection loss between the gRPC client and server, temporary connection loss between the gRPC server and the current service leader, or service leader changes. This happens at the slightest suspicion that a notification might have been lost. To restore the subscription, client code must issue a new `DescribeSemaphore` call, properly managing the situation where the result of the new call might differ (for example, if the notification was indeed lost).

## Examples

* [Distributed lock](../../recipes/ydb-sdk/distributed-lock.md)
* [Leader election](../../recipes/ydb-sdk/leader-election.md)
* [Service discovery](../../recipes/ydb-sdk/service-discovery.md)
* [Configuration publication](../../recipes/ydb-sdk/config-publication.md)
