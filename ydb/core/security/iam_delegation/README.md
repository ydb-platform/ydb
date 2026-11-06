# Node-local IAM delegation control service

The service submits setup/revoke once and polls known operation IDs until
completion or the absolute polling deadline. Transport ambiguity never justifies
replaying a mutation. A failed call does not prove no delegation was created;
callers own durable recovery. This service does not persist delegation state.

`IamConfig` is a constructor-owned startup snapshot. Configure the service
identity, `ServiceControlEndpoint` and `SystemTokenName`, selecting an existing
enabled token-manager provider. The feature flag can start the node service
dynamically; configuration changes require restart. Turning the flag off leaves
an already running service available for outstanding requests.

Setup, revoke and polling share one standard `TBackoff` in the node service.
Healthy calls run concurrently. A failure advances the version and deadline;
waiters recheck that version before one recovery call proceeds. Late outcomes
cannot reset a newer failure. A finite reservation permits recovery after a
caller disappears. This state is volatile and resets on node restart. Durable
cluster-wide scheduling belongs to the later service-tablet implementation.

Per-request deadlines, requests and intermediate results live in coroutine
frames. Cancellation during cooldown sends no IAM request. Local token-provider
failure does not assert an IAM service outage. Credentials are read again after
cooldown so rotation is observed before dispatch.

Errors distinguish rejected credentials from missing rights and include fixed
method-specific requirements. Remote messages, arbitrary detail types, token
manager diagnostics and credentials are excluded from returned diagnostics.

Tests in `ut` cover request forwarding and mutation uncertainty, raw-error
redaction, token updates, real gRPC integration, startup/config notifications,
shared scheduling across operations, stale reports, exact deadlines and shutdown.
Shared helper timer ownership and cancellation tests live in
`core/util/async_backoff_ut`. Existing event IDs/config fields are preserved;
the existing request/result API is retained. This PR adds no tablet type or persisted schema.
