# IAM delegation tablet: Fenced revocation workers

This tablet owns IAM delegation metadata independently of SchemeShard and tenant teardown. It stores identities and operation state, never user/IAM tokens or service account keys.

ClaimRevocations leases a bounded batch of eligible durable cleanup work. Tokens include tablet generation, attempt, worker and deadline. FinishRevocation accepts only the current unexpired claim; success retains a terminal record, while retry schedules a tablet-clock due time. Old acknowledgements cannot overwrite newer work. Workers must wait for an authoritative completed IAM result before acknowledging success.

## Deployment and protocol

Provision `TTabletTypes::IamDelegation` under a service-owned Hive owner with storage that survives managed tenants. Factory registration does not create a tablet. There are no SchemeShard lifecycle changes or per-tenant provisioning hooks.

The trusted internal API is `TEvIamDelegationTablet::TEvRequest` / `TEvResponse`; commands are typed protobuf alternatives: `RegisterDatabase`, CREATE/ALTER `Stage`, `BindSecret`, `StartSetup`, `SetSetupResult`, `Promote`, `DropSecret`, `GetSecret`, `GetDelegation`, `ClaimRevocations`, `FinishRevocation`. Lifecycle choices use enums, with revisions guarding concurrent mutations. Public callers must authenticate and authorize before forwarding requests.

Database incarnations must be immutable and never reused, including after path reuse; derive them from the full domain root PathId or an equally strong identity. Database and secret paths must be canonical absolute paths. The registry retains external aliases after tenant teardown.

IAM node callers, KQP/schema orchestration, secret readers, service discovery and the administrative CMS export RPC are later integration changes. ydbcp is unchanged. No delegation feature is enabled merely by this tablet slice.

## Validation

Lease expiry, tablet-generation fencing, attempt fencing, durable retries and unknown-setup exclusion, in addition to lifecycle/reboot coverage.

```sh
./ya make --build relwithdebinfo -tA ydb/core/tx/iam_delegation/ut
```

Tests use the real tablet storage/runtime and no live IAM service.
