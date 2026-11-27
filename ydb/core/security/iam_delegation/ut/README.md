# IAM delegation behavior scenarios

The tests exercise the real service actor, using virtual time and intercepted IAM requests. The integration cases in `iam_delegation_ut.cpp` also exercise the real gRPC clients against a local mock server. They do not validate live cloud policy or IAM credentials.

These scenarios define the shared contract for coroutine and classic event implementations. `IamEventArbitration` publishes captured real scheduler events and uses `viaActorSystem=true` to queue replies and token updates without running the mailbox inline. The same tests run unchanged against both implementations.

| Scenario | Required observable behavior |
|---|---|
| Unrelated poll wake, then queued setup/revoke success, then own RPC timeout | The successful reply wins while the absolute attempt deadline is unexpired, even when mailbox processing is delayed. |
| Own RPC timeout before queued success | TIMEOUT with unknown remote outcome; no mutation replay. |
| Unrelated poll wake before queued credentials | The credential update can win over its own later timeout event; another request's wake cannot expire it. |
| Retired wait timer delivered during a later poll | The timer cannot complete or expire the later wait. |
| Obsolete RPC reply before queued recovery success | The obsolete event cannot admit another probe; the valid success releases all current waiters. |
| Gate wake before an expired caller's own attempt-timeout event | Admission occurs first, then the absolute dispatch guard returns the polling-deadline error; its recovery reservation remains in effect. |
| Attempt-timeout event before gate wake | Return the service-retry-deadline error without consuming a recovery reservation. |
| Expiring logical backoff timer owner before another mailbox event | Notify one FIFO successor in the same actor turn; cancelling an ordinary waiter does not grant admission. |
| Unexpected correlated reply after the absolute attempt deadline | Return TIMEOUT before classifying the unexpected response. |

Other suites retain request forwarding, cookie correlation, token invalidation and rotation, fixed/redacted authentication and permission diagnostics, known-operation polling, transient versus terminal failures, absolute poll boundaries, bulk waiter expiry, graceful poison and hard teardown. The eight arbitration tests supplement those assertions rather than replacing them.

Internal RPC-cookie values and timer allocation are implementation details. Public caller cookies, request/result types, configuration, wire field numbering and persisted formats remain unchanged. Source-reviewed recoverable-allocation paths are not a claim of executed fault injection. Cluster rolling tests with the feature disabled do not prove live IAM mixed-version behavior.
