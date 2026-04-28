# Distributed lock

Consider a scenario where it is necessary to ensure that only one instance of a client application accesses a shared resource at any given time. To achieve this, the semaphore mechanism in [{{ ydb-short-name }} coordination nodes](../../reference/ydb-sdk/coordination.md) can be utilized.

## Semaphore lease mechanism

In contrast to local multithreaded programming, clients in distributed systems do not directly acquire locks or semaphores. Instead, they lease them for a specified duration, which can be periodically renewed. Due to reliance on physical time which can vary between machines, clients and the server might encounter situations where multiple clients believe they have acquired the same semaphore simultaneously, even if the server's perspective differs. To reduce the likelihood of such occurrences, it is crucial to configure automatic time synchronization beforehand, both on servers hosting client applications and on the {{ ydb-short-name }} side, ideally using a unified time source.

Therefore, while distributed locking through such mechanisms cannot guarantee the complete absence of simultaneous resource access, it can significantly lower the probability of such events. This approach serves as an optimization to prevent unnecessary competition among clients for a shared resource. Absolute guarantees against concurrent resource requests сould be implemented on the resource side.

## Code example

{% list tabs %}

- Go

   ```go
   for {
     if session, err := db.Coordination().CreateSession(ctx, path); err != nil {
       return fmt.Errorf("cannot create session: %v", err);
     }

     if lease, err := session.AcquireSemaphore(ctx,
       semaphore,
       coordination.Exclusive,
       options.WithEphemeral(true),
     ); err != nil {
       // the session is likely lost, try to create a new one and get the lock in it
       session.Close(ctx);
       continue;
     }

     // lock acquired, start processing
     select {
        case <-lease.Context().Done():
     }

     // lock released, end processing
   }
   ```

- Python

  {% list tabs %}

  - Native SDK

    ```python
    import ydb

    def coordination_service_workflow(driver: ydb.Driver, node_path: str, semaphore_name: str):
        client = driver.coordination_client

        client.create_node(node_path)

        with client.session(node_path) as session:
            with session.semaphore(semaphore_name) as semaphore:
                print("Some exclusive work")

    ```

  - Native SDK (Asyncio)

    ```python
    import os
    import ydb

    async def coordination_service_workflow(driver: ydb.aio.Driver, node_path: str, semaphore_name: str):
        client = driver.coordination_client
        await client.create_node(node_path)
        async with client.session(node_path) as session:
            async with session.semaphore(semaphore_name) as semaphore:
                print("Some exclusive work")
    ```

  {% endlist %}

- JavaScript

  ```javascript
  import { Driver } from '@ydbjs/core'
  import { CoordinationClient } from '@ydbjs/coordination'

  const driver = new Driver('grpc://localhost:2136/local')
  const client = new CoordinationClient(driver)

  await using session = await client.createSession('/local/my-app')
  await using lock = await session.mutex('job-lock').lock()
  await doWork(lock.signal)

  // For long lived applications

  for await (let session of client.openSession('/local/my-app')) {
    let mutex = session.mutex('job-lock')

    try {
      // Blocks until the lock is acquired.
      await using lock = await mutex.lock()

      await doWork(lock.signal)
    } catch {
      if (session.signal.aborted) continue // session expired, retry
      throw error
    }

    break
  }
  ```

{% endlist %}