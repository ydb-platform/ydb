# Working with coordination nodes

This article describes how to use the {{ ydb-short-name }} SDK to coordinate the work of multiple client application instances using [coordination nodes](../../concepts/datamodel/coordination-node.md) and their semaphores.

## Creating a coordination node

Coordination nodes are created in {{ ydb-short-name }} databases in the same namespace as other schema objects, such as [tables](../../concepts/datamodel/table.md) and [topics](../../concepts/topic.md).

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

{% endlist %}

## Working with sessions {#session}

### Creating a session {#create-session}

To start working with coordination nodes, a client must establish a session within which it will perform all operations with the coordination node.

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

   - `Description` - a text description of the session, displayed in internal interfaces and can be useful for problem diagnosis.
   - `OnStateChanged` - called on significant changes during the session's life, passing the corresponding state:

     - `ATTACHED` - the session is connected and operating in normal mode.
     - `DETACHED` - the session temporarily lost connection with the service but can still be restored.
     - `EXPIRED` - the session lost connection with the service and cannot be restored.

   - `OnStopped` - called when the session stops attempting to restore the connection with the service, which can be useful for establishing a new connection.
   - `Timeout` - the maximum timeout during which the session can be restored after losing connection with the service.

{% endlist %}

### Session control {#session-control}

It's important for the client application to monitor the session state, as it can only rely on the state of captured semaphores while the session is alive. When the session ends by client or server initiative, the client can no longer assume that other clients in the cluster haven't captured its semaphores and changed their state.

{% list tabs %}

- Go

  In Go SDK, the session context `session.Context()` is used to track such situations, which terminates along with the session. The SDK can handle transport-level errors on its own and re-establish connection with the service, trying to restore the session if still possible. Thus, the client only needs to monitor the session context to react timely to its loss.

- C++

  In the C++ SDK, an active session continuously maintains and automatically re-establishes the connection with the {{ ydb-short-name }} cluster in the background.

{% endlist %}

## Working with semaphores {#semaphore}

### Creating a semaphore {#create-semaphore}

When creating a semaphore, you can specify its limit. The limit determines the maximum value to which it can be increased. Calls attempting to increase the semaphore value above this limit will wait until their increase requests can be fulfilled without exceeding the semaphore's limit.

{% list tabs %}

- Go

    ```go
    err := session.CreateSemaphore(ctx,
        "my-semaphore", // semaphore name
        10              // semaphore limit
    )
   ```

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

{% endlist %}

### Acquiring a semaphore {#acquire-semaphore}

To acquire a semaphore, the client must call the `AcquireSemaphore` method and wait for a special `Lease` object. This object represents confirmation that the semaphore value was successfully increased and can be considered as such until explicit release of such semaphore or termination of the session in which such confirmation was received.

{% list tabs %}

- Go

    ```go
    lease, err := session.AcquireSemaphore(ctx,
        "my-semaphore",  // semaphore name
        5,              // value to increase semaphore by
    )
    ```

    Similar to the session, the `Lease` object also has a context that terminates at one of these moments.

    To cancel waiting for semaphore acquisition, it's sufficient to cancel the passed context `ctx`.

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

{% endlist %}

The taken value of an acquired semaphore can be decreased (but not increased) by calling the `AcquireSemaphore` method again with a smaller value.

### Updating semaphore data {#update-semaphore}

Using the `UpdateSemaphore` method, you can update (replace) the semaphore data that was attached during its creation.

{% list tabs %}

- Go

    ```go
    err := session.UpdateSemaphore(
        "my-semaphore",                                   // semaphore name
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

{% endlist %}

This call doesn't require acquiring the semaphore and doesn't lead to it. If you need only one specific client to update the data, this must be explicitly ensured, for example, by acquiring the semaphore, updating the data, and releasing the semaphore back.

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

{% endlist %}

### Releasing a semaphore {#release-semaphore}

{% list tabs %}

- Go

    To release a semaphore acquired in a session, call the `Release` method on the `Lease` object.

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
