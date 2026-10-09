# IAM delegation tablet: Tablet foundation and database registry

This tablet owns IAM delegation metadata independently of SchemeShard and tenant teardown. It stores identities and operation state, never user/IAM tokens or service account keys.

The tablet boots through the standard Hive/local factories and durably registers immutable database incarnations and their path/cloud aliases. Repeating the same registration is idempotent; changing its identity is rejected. The cloud database ID is optional for self-hosted installations.

## Deployment and protocol

Provision `TTabletTypes::IamDelegation` under a service-owned Hive owner with storage that survives managed tenants. Factory registration does not create a tablet. There are no SchemeShard lifecycle changes or per-tenant provisioning hooks.

The trusted internal API is `TEvIamDelegationTablet::TEvRequest` / `TEvResponse`; commands are typed protobuf alternatives: `RegisterDatabase`. Lifecycle choices use enums, with revisions guarding concurrent mutations. Public callers must authenticate and authorize before forwarding requests.

Database incarnations must be immutable and never reused, including after path reuse; derive them from the full domain root PathId or an equally strong identity. Database and secret paths must be canonical absolute paths. The registry retains external aliases after tenant teardown.

IAM node callers, KQP/schema orchestration, secret readers, service discovery and the administrative CMS export RPC are later integration changes. ydbcp is unchanged. No delegation feature is enabled merely by this tablet slice.

## Validation

Registration, canonical-path validation, incarnation isolation and reboot durability.

```sh
./ya make --build relwithdebinfo -tA ydb/core/tx/iam_delegation/ut
```

Tests use the real tablet storage/runtime and no live IAM service.
