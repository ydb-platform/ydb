# IAM delegation tablet: Atomic secret lifecycle and durable revocation outbox

This tablet owns IAM delegation metadata independently of SchemeShard and tenant teardown. It stores identities and operation state, never user/IAM tokens or service account keys.

ALTER stages a replacement while keeping the current delegation active. Promote atomically makes a successful setup current and enqueues its predecessor for revocation. Drop retires current/pending bindings and releases the name. Unknown in-flight setup stays in WAITING_SETUP until an authoritative outcome; a timeout cannot prove setup will not finish. Terminal identities remain as permanent idempotency tombstones.

## Deployment and protocol

Provision `TTabletTypes::IamDelegation` under a service-owned Hive owner with storage that survives managed tenants. Factory registration does not create a tablet. There are no SchemeShard lifecycle changes or per-tenant provisioning hooks.

The trusted internal API is `TEvIamDelegationTablet::TEvRequest` / `TEvResponse`; commands are typed protobuf alternatives: `RegisterDatabase`, CREATE/ALTER `Stage`, `BindSecret`, `StartSetup`, `SetSetupResult`, `Promote`, `DropSecret`, `GetSecret`, `GetDelegation`. Lifecycle choices use enums, with revisions guarding concurrent mutations. Public callers must authenticate and authorize before forwarding requests.

Database incarnations must be immutable and never reused, including after path reuse; derive them from the full domain root PathId or an equally strong identity. Database and secret paths must be canonical absolute paths. The registry retains external aliases after tenant teardown.

IAM node callers, KQP/schema orchestration, secret readers, service discovery and the administrative CMS export RPC are later integration changes. ydbcp is unchanged. No delegation feature is enabled merely by this tablet slice.

## Validation

Atomic promotion and predecessor retirement, DROP with unknown setup, stale drops, failed ALTER preservation, name release and permanent identity tombstones across reboot.

```sh
./ya make --build relwithdebinfo -tA ydb/core/tx/iam_delegation/ut
```

Tests use the real tablet storage/runtime and no live IAM service.
