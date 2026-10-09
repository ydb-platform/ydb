# IAM delegation tablet: Durable CREATE intent and IAM setup bookkeeping

This tablet owns IAM delegation metadata independently of SchemeShard and tenant teardown. It stores identities and operation state, never user/IAM tokens or service account keys.

CREATE persists a globally unique operation ID and the complete IAM binding before any schema or IAM side effect. Bind the verified schema object, durably start setup once, then record its unknown or final outcome. Full secret PathId and database incarnation identify the immutable object. Original requests and referrers are retained for idempotency and cannot be reused. Setup does not dispatch IAM calls.

## Deployment and protocol

Provision `TTabletTypes::IamDelegation` under a service-owned Hive owner with storage that survives managed tenants. Factory registration does not create a tablet. There are no SchemeShard lifecycle changes or per-tenant provisioning hooks.

The trusted internal API is `TEvIamDelegationTablet::TEvRequest` / `TEvResponse`; commands are typed protobuf alternatives: `RegisterDatabase`, CREATE `Stage`, `BindSecret`, `StartSetup`, `SetSetupResult`, `GetSecret`, `GetDelegation`. Lifecycle choices use enums, with revisions guarding concurrent mutations. Public callers must authenticate and authorize before forwarding requests.

Database incarnations must be immutable and never reused, including after path reuse; derive them from the full domain root PathId or an equally strong identity. Database and secret paths must be canonical absolute paths. The registry retains external aliases after tenant teardown.

IAM node callers, KQP/schema orchestration, secret readers, service discovery and the administrative CMS export RPC are later integration changes. ydbcp is unchanged. No delegation feature is enabled merely by this tablet slice.

## Validation

Durable CREATE intent/replay, exact binding retention, path confinement, binding and setup preconditions, stale revisions and known IAM operation identity.

```sh
./ya make --build relwithdebinfo -tA ydb/core/tx/iam_delegation/ut
```

Tests use the real tablet storage/runtime and no live IAM service.
