# Ydb provider: direct remote table reads

`SOURCE_TYPE='Ydb'` reads remote YDB row tables directly through Query Service
using Query SDK. The provider category is `ydb`. The remote object kind selects
the provider: tables use Query SDK, topics use PQ/Topic SDK.
`Generic.Connector.DatabaseNames` and the presence of a Connector client no
longer select the implementation for YDB tables. There is no fallback to
Connector.

Enable `Ydb` through `AllExternalDataSourcesAreAvailable` or
`AvailableExternalDataSources`. SQL definitions, EXPLAIN `SourceType`, and the
serialized runtime source name all use `Ydb`. The Query SDK implementation owns
`providers/ydb`, with protobuf messages in `NYql.NYdb`.

The previous scan provider is archived in `providers/ydb/deprecated`. It has no
active includes, build dependencies, registrations or parent `RECURSE` entry.
KQP uses `kikimr` for local tables; `ydb` now belongs to the remote Query SDK
provider and is no longer rewritten to `kikimr`. Global `PRAGMA ydb.*` query
settings continue to configure KQP; only their configuration nodes are routed
to the KQP settings handler, independently of the remote table provider.
This changes runtime plans and
protobuf type URLs; all nodes executing these plans must support the new names.

## Connection and authentication

Provide `LOCATION` as `host:port`, an absolute `DATABASE_NAME`, and `USE_TLS`
(`true` or `false`, default `false`). TLS uses certificate and hostname validation
against the driver's trusted roots. The initial endpoint is used directly;
endpoint discovery and database-ID resolution are not supported. Existing `Ydb`
definitions with relative database names are normalized to absolute SDK paths
(e.g. `local` becomes `/local`).

The direct path supports `AUTH_METHOD='TOKEN'` with `TOKEN_SECRET_PATH`, and
`AUTH_METHOD='NONE'`. The token principal must have permission to describe and
read the remote table. Tokens travel through secure parameters, not serialized
source settings. `Ydb` retains its existing DDL authentication and property
contract because the same EDS can also serve topics. For table reads, the Query
SDK provider rejects BASIC, SERVICE_ACCOUNT and IAM authentication, and `DATABASE_ID` resolution at
compilation. `SHARED_READING` and `SHARED_READING_GROUP` affect topics and are
ignored for table reads. Database paths for table reads must be absolute and
cannot contain empty, `.` or `..` components. Hostname allowlists apply at DDL
validation.

For an existing secret `remote_token`:

```sql
CREATE EXTERNAL DATA SOURCE remote_db WITH (
    SOURCE_TYPE = 'Ydb',
    LOCATION = 'remote.example:2135',
    DATABASE_NAME = '/Remote',
    AUTH_METHOD = 'TOKEN',
    TOKEN_SECRET_PATH = 'remote_token',
    USE_TLS = 'true'
);

SELECT Key FROM remote_db.`items`;
```

The experimental direct path has a fixed internal 60-second limit for the entire
source read, including retries and time waiting for a slow downstream consumer
(backpressure). This is an experimental limitation, not an inactivity timeout.
Increasing the query or script timeout does not extend it. There is no external
data source option to configure this limit; `READ_TIMEOUT_MS` is rejected
for both `Ydb` and `Ydb` sources. Propagating the query deadline through
the entire remote read path is future work. Metadata loading has a separate
finite local budget.

## Query and schema support

Reads support ordinary row tables with Bool, signed and unsigned 8/16/32/64-bit
integers, Float, Double, String and Utf8 columns, including nullable columns.
Column tables, Decimal, PostgreSQL types, date/time types, Json, JsonDocument,
Yson, Uuid, DyNumber and other types are not supported. Validation applies to the
whole table schema: any unsupported column rejects the table, even when the query
does not select that column. Unsupported columns are never silently removed.

Projection is pushed into the remote SELECT. Columns needed by local filters,
joins or sorting are retained. `COUNT(*)` and constant projections read one
physical carrier column to preserve the number of rows. Query plans expose the
physical projection as `ReadColumns` and the fixed internal source timeout as
`ReadTimeoutMs` (60000 ms).
Filters, limits, aggregation and joins execute locally; they are not pushed into
the remote query.

One source uses one Query Service stream and one snapshot; reads are not split
into parallel partitions. Separate source reads do not share a remote snapshot.
Retries are attempted only before any rows are delivered and retain the original
local deadline. The current retry set includes `ABORTED`, so deterministic remote
errors reported with that status are not yet distinguished. Writing
to the remote external source, streamlookup joins and reads from
`CREATE/ALTER STREAMING QUERY` are unsupported.
Existing `Ydb` table queries that rely on Connector type coverage, filter
pushdown, database-ID resolution, other authentication methods, streamlookup or
streaming reads need migration before adopting this implementation. These
capabilities are not supplied by the current Query SDK table reader.

Provider registration does not create SDK drivers. Metadata loading acquires a
shared metadata-client cache on first use; plaintext and TLS use independent
drivers. Runtime source registration is unconditional so executing a prepared
plan does not depend on current compilation-time availability.

## Remote capability and resource limits

The remote Query Service must support `ExecuteQuery` results in Arrow format
with schema inclusion on every part and uncompressed Arrow IPC batches. A server
that does not support or enable that capability can describe a table successfully
and still reject its read. Check the remote server's capability and configuration;
no server-version compatibility guarantee is implied by this experimental path.

Current limits are 64 distinct metadata tables per compilation, 1024 columns per
table and 64 KiB of compact metadata schema. A received gRPC message and decoded
Arrow part are each limited to 64 MiB. Output blocks target 1 MiB; one larger row
may occupy its own block up to 32 MiB. A larger row fails explicitly. These are
acceptance limits, not resource-manager reservations or a bound on peak process
memory: SDK parsing and an input part, compact output and task-allocator copy can
coexist. A consumer that retains blocks remains subject to its task memory limit.
