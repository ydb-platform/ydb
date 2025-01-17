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

{% endlist %}