# Database freeze through CMS

`CmsService.AlterDatabase` accepts an optional `freeze` flag. `true` closes
row-table write paths and waits for outstanding writes, asynchronous index
changes and CDC delivery to topics. Reads remain available. `false` removes
the database freeze. Column tables are unsupported.

A freeze request contains only `path`, `freeze`, and optionally
`operation_params`, `generation` and `idempotency_key`. Combining it with any
other database setting, including an unknown protobuf field, is rejected.
`cancel_after` and `forget_after` are unsupported.

## Operation contract

With `operation_mode=ASYNC`, AlterDatabase returns a durable operation id with
`ready=false`. Poll `OperationService.GetOperation` using that id until
`ready=true`. Only a successful completed operation confirms application to
all participants. `SYNC` subscribes to that same operation and waits for its
completion. An RPC timeout or disconnect does not undo the persisted request;
retrying with the same idempotency key recovers its operation id.

The C++ SDK exposes `TAlterDatabaseSettings::Freeze(bool)`. Its AlterDatabase
future waits for completion, including polling an asynchronous operation.
An unset setting leaves existing AlterDatabase behavior unchanged. Explicit
`false` is serialized and requests thaw. New gRPC endpoints and the SDK reject
an old server's immediate success without an operation id as unsupported.

## Coordination and recovery

Console persists the database incarnation, epoch, requested state, idempotency
key and result separately from create/remove operations. It resolves the
owning SchemeShard through SchemeCache and polls the internal protocol
[described here](../../tx/schemeshard/database_freeze.md). A dedicated tenant
SchemeShard receives its own local root path; a shared SchemeShard receives
the database's domain path.

Pending operations restart after Console recovery. Completed operation ids
and idempotency keys remain available after thaw. A key reused with a different
freeze value is rejected; a successful old freeze id does not request another
freeze after thaw. Requests without a key reuse a pending operation with the
same requested state. A newer conflicting request supersedes a pending
operation, marks it cancelled and uses a newer epoch. Late replies from its
worker cannot finish the new request.

Completed operation records currently have no automatic expiration.
`OperationService.ForgetOperation` is unsupported for these operations.

Other AlterDatabase changes and database removal are rejected while a freeze
operation is pending. A frozen database or a failed freeze/thaw must be
explicitly thawed before removal. Failures do not implicitly reopen write
paths. A successful thaw reopens schema admission after every DataShard has
acknowledged.

## Validation

`ydb/core/grpc_services/ut_cms` checks the public CMS and Operation services,
CDC acknowledgements, resolved records, initial scans, freeze/thaw, schema
admission, persistence, idempotency, failures and timeout recovery. SDK tests
in `ydb/public/sdk/cpp/tests/unit/client/cms` check optional flag serialization,
operation polling, compatibility with old endpoints and error propagation.
Console authorization and operation-notification tests cover the existing
administrative access and completion-subscription paths.
