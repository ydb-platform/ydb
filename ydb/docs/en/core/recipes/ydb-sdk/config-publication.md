# Configuration publication

Let's consider a scenario where we need to publish a small configuration for multiple application instances that should promptly react to its changes.

This scenario can be implemented using semaphores in [{{ ydb-short-name }} coordination nodes](../../reference/ydb-sdk/coordination.md) as follows:

1. A semaphore is created (for example, named `my-service-config`).
1. The updated configuration is published through `UpdateSemaphore`.
1. Application instances call `DescribeSemaphore` with `WatchData=true`. In the result, the `Data` field will contain the current version of the configuration.
1. When the configuration changes, `OnChanged` is called. In this case, application instances make a similar `DescribeSemaphore` call and receive the updated configuration.