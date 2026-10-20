# Per-call monitoring

The guest supplies an optional alias on **each** `WriteRequest` call (the last
argument). Aliases describe operations, not concrete URLs or parameter values;
they do not change routing or authorization. Different calls from one guest
invocation can use different aliases. They are not manifest properties.

Request wire v4 adds the alias after method/path and before payload. Native
transport also accepts the original v3 request layout and labels it `unlabeled`.
Responses stay v3; service rows stay ABI v2. A new guest needs the v4-capable host.
Allocate guest request buffers with `RequestSize`, including the alias.

Aliases have at most 64 ASCII letters, digits, dots, underscores or hyphens.
Empty aliases use `unlabeled`; invalid aliases and aliases beyond the registry
limit use `other`. At most 128 distinct user aliases per protocol plus these two
fixed groups exist for the registry lifetime. The registry never remembers
rejected alias values. These bounds are separate from per-operation memory
reservations. Do not put IDs, credentials, URL parameters or other sensitive
values into an alias, even if they satisfy this syntax.

All operations use the same schema. Labels are `protocol=http|grpc|internal` and
`call_alias`; only outcome/rejection counters add their respective fixed label.

| Metric | Type | Meaning |
| --- | --- | --- |
| `RequestsStarted` | Counter | Accepted operations, including dependency waits |
| `RequestsCompleted{outcome}` | Counter | Exactly one of `success`, `error`, `timeout`, `cancelled` |
| `RequestsInFlight` | Gauge | Accepted but not logically completed |
| `RequestDurationMs` | Histogram | Acceptance to logical completion, including dependency and queue waits |
| `QueueDelayMs` | Histogram | Time between a readiness/scheduling notification and its processing on the user pool |
| `RequestPayloadBytes` | Histogram | Application payload bytes when available |
| `ResponsePayloadBytes` | Histogram | Delivered application payload bytes when available |
| `ReservedBytes` | Gauge | Admitted transport reservation until physical cleanup |
| `AdmissionRejected{reason}` | Counter | `local_operations`, `local_bytes`, `shared_operations`, `shared_bytes`, `closed`, `deadline` |
| `CleanupPending` | Gauge | Logically completed operations whose physical owner still exists |
| `CleanupDurationMs` | Histogram | Logical completion to last physical owner release |

Admission rejection does not increment started/completed; malformed envelopes
cannot be assigned a trusted call label and are not counted as admitted calls.
Transport success follows existing HTTP/gRPC status handling; guest decoding can
subsequently fail without turning a successful network operation into an error.
Queue-delay samples describe executor progress turns, not a network phase or a
sum to subtract from latency. Counters/histograms update on events, with no
periodic metric poller. Time uses a monotonic clock.

Logical cancellation completes the metric immediately, before executor/native
cleanup. Cancellation at or after the shared absolute deadline is counted as
`timeout`; an earlier cancellation remains `cancelled` even if cleanup crosses
the deadline. Logical cancellation can precede native cleanup. Watch `CleanupPending` and
`ReservedBytes` together; zero in-flight alone does not prove resources are free.
Memory reservations are conservative credits, not measured RSS. Payload sizes
exclude framing, headers and credentials. Operations without exposed payloads
leave the size histograms without observations rather than inventing byte counts.

Compare duration histograms by alias/protocol to identify slow operations. This
aggregate monitoring does not identify an individual SQL query or request ID.
Use query diagnostics/tracing for individual executions; do not add unbounded
query IDs as metric labels. Deployment dashboards and workload-specific alert
thresholds can be supplied separately from the transport/SDK stack.

Authorization uses the same logical completion boundary as its parent: pending
`auth.tvm` metrics finish immediately on cancellation/deadline, even when the user
pool cannot yet process cleanup. Already completed authorization keeps its outcome.
The parent's transport reservation is not charged again to authorization, whose
`ReservedBytes` is zero. A late provider callback can retain the auth metric and
its `CleanupPending` after the parent request and its reservation have been freed;
it does not retain the parent transport buffers or lease.
