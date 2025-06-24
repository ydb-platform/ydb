# Leader election

Consider a scenario where multiple application instances need to elect a leader among themselves and be aware of the current leader at any given time.

This scenario can be implemented using semaphores in [{{ ydb-short-name }} coordination nodes](../../reference/ydb-sdk/coordination.md) as follows:

1. A semaphore is created (for example, named `my-service-leader`) with `Limit=1`.
1. All application instances call `AcquireSemaphore` with `Count=1`, specifying their endpoint in the `Data` field.
1. Only one application instance's call will complete quickly, while others will be queued. The application instance whose call completes successfully becomes the current leader.
1. All application instances call `DescribeSemaphore` with `WatchOwners=true` and `IncludeOwners=true`. The result's `Owners` field will contain at most one element, from which the current leader's endpoint can be determined via its `Data` field.
1. When the leader changes, `OnChanged` is called. In this case, application instances make a similar `DescribeSemaphore` call to learn about the new leader.