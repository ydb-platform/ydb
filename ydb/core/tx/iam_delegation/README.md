# IAM delegation tablet

This tablet owns the durable inventory and revocation outbox for IAM delegation
secrets. It stores identities and operation state, never user tokens, IAM tokens
or service account keys. It is independent of SchemeShard and tenant teardown.

This first change provides the storage protocol, tablet factory and recovery
tests. IAM callers, KQP orchestration, secret readers and the CMS export RPC are
separate integration changes. The tablet does not enable delegation secrets on
its own.

## Placement and identity

The tablet uses `TTabletTypes::IamDelegation` and the normal Hive/local tablet
factory. Provision it under a service-owned Hive owner, with storage channels
that survive the managed tenants. A tenant must not own this tablet's lifetime.
There is no automatic per-database provisioning or SchemeShard lifecycle hook.
Service discovery and the deployment configuration belong to the integration
change; merely registering a factory does not create a tablet.

Register each database before creating an intent. Its incarnation identifies one
immutable database object: the caller must derive it from the full domain root
PathId or an equally strong identity. A path or cloud database ID alone is not an
incarnation. Preserve the registered path and database ID for export after
teardown. Registration also distinguishes a known empty inventory from an
unknown database. The cloud database ID is optional for self-hosted installations.
Database and secret paths must be canonical absolute paths, with no dot-only
components. A secret path must be a strict descendant of its registered database.

A bound secret uses its database incarnation and full `(PathOwnerId,
PathLocalId)` identity. A pending CREATE additionally records its schema
transaction ID and path before that identity is available. A caller must verify
the schema transaction and resulting object before binding it; the tablet does
not perform schema authorization or prove the outcome of a schema transaction.

The operation ID is a globally unique idempotency key. The full original IAM
binding is retained with it, including service, microservice, resource, account,
referrer and the typed reference policy. Cleanup must use that saved binding,
even after configuration changes. Referrers are never reused.

## Protocol

The internal actor API is `TEvIamDelegationTablet::TEvRequest` /
`TEvResponse`, with typed commands in a protobuf `oneof`. It is a trusted
internal tablet protocol. Public handlers must authenticate and authorize their
callers before forwarding requests; knowing a tablet ID is not an authorization
mechanism.

| Command | Purpose |
| --- | --- |
| `RegisterDatabase` | Retain the immutable incarnation and external aliases. |
| `Stage` | Persist a CREATE or ALTER intent and the complete IAM binding. |
| `BindSecret` | Attach a CREATE intent to the verified schema object. |
| `StartSetup` | Durably record that IAM setup may be dispatched. |
| `SetSetupResult` | Record an unknown or definitive setup outcome. |
| `Promote` | Activate a successful setup and atomically queue its predecessor. |
| `DropSecret` | Retire a secret or an unbound CREATE intent. |
| `GetSecret`, `GetDelegation` | Read authoritative lifecycle state. |
| `ListInventory` | Enumerate retained nonterminal delegation identities. |
| `ClaimRevocations` | Lease a bounded batch of eligible cleanup work. |
| `FinishRevocation` | Acknowledge success or schedule a retry with a claim token. |

Lifecycle states, CREATE/ALTER modes, setup outcomes, reference policy and
revocation outcomes are enums. Boolean options must not encode these state
machines. Compare-and-swap revisions protect mutations against stale callers.

For CREATE, persist `Stage` before creating the ordinary schema secret shell.
After verifying its existing schema transaction identity, call `BindSecret`.
Only the first successful `StartSetup` permits dispatch to IAM; a repeated start
is rejected and requires outcome recovery. Report the final setup
outcome and promote it. ALTER keeps serving the current binding while a new
intent is pending. Promotion and enqueueing the old referrer share one tablet
transaction.

Retirement must not forget an in-flight setup. If setup has started and its
outcome is unknown, retain it in `WAITING_SETUP`; it is not eligible for
revocation claims. A caller must resolve the IAM operation before reporting a
definitive outcome. Neither a lease expiry nor a timeout proves that IAM cannot
complete setup later. Setup dispatch/retry coordination is the responsibility of
the integration layer; this tablet does not provide exactly-once remote calls.

Successful setup can be queued for revocation after retirement. A definite
failure with no IAM side effect can be cancelled. Terminal records retain
identity/idempotency information; this initial implementation has no garbage
collection policy that could allow stale work to recreate a referrer.

Claim tokens include the tablet generation and attempt number. A delayed
acknowledgement must not release or complete a newer claim. Due times and lease
deadlines are calculated from the tablet's clock. A worker reports an IAM revoke
as successful only after its operation completes or IAM authoritatively reports
that the delegation is absent. A failed revoke remains durable cleanup work.

## Inventory and future CMS export

Inventory includes active, pending, unknown-setup and revoking identities,
including those whose secrets have disappeared. It is not reconstructed by
walking schema objects or by deduplicating service accounts.

The initial internal API uses optimistic pagination: continuation requests carry
the inventory revision from the first page. A mutation invalidates that revision
and returns `SNAPSHOT_EXPIRED`; the caller restarts enumeration. This explicitly
avoids mixing different inventories. It does not retain an immutable snapshot
for arbitrary concurrent mutation. Nonfinal pages must be full so that ydbcp's
existing short-page termination rule cannot lose records. The internal limit is
1,000 records per page; unsupported sizes are rejected rather than clamped.
Terminal records remain available through `GetDelegation` and idempotent replay,
but are removed from the inventory index.

The follow-up public RPC is intended to be
`Ydb.Cms.V1.CmsService.ListDatabaseIamDelegations`, with the normal CMS `path`
argument, YDB operation/result envelope, and opaque continuation tokens bound to
the database incarnation, revision and cursor. It will
use the surviving administrative endpoint and enforce administrative permissions.
Exported `cloud_id`, `service_account_id` and complete referrer/binding fields
allow ydbcp to construct its existing IAM revoke request shape. Secret referrers
have type `ydb.secret`; ydbcp's current database revoke helper hardcodes
`ydb.database`, so that helper cannot revoke these unchanged.

Listing does not stop new setup, resolve unknown IAM operations, revoke anything,
or acknowledge cleanup. The database deletion caller must establish those
boundaries before treating an inventory as a complete cleanup set. Changes to
ydbcp are outside this implementation.

## Validation

Run the real tablet storage and reboot tests with:

```sh
./ya make --build relwithdebinfo -tA ydb/core/tx/iam_delegation/ut
```

The tests exercise transaction boundaries and recovered state, using the actor
runtime clock for lease expiry. No live IAM service is used.
