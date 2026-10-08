# YdbExternal provider: direct remote reads

`SOURCE_TYPE='YdbExternal'` reads remote YDB row tables directly through Query
Service. The internal provider name is `ydb_external`. This experimental source
uses the standard external-source availability settings:
`AllExternalDataSourcesAreAvailable`, or `YdbExternal` in
`AvailableExternalDataSources`. It has no separate feature flag.

`SOURCE_TYPE='Ydb'` continues to use Generic/Connector and its existing topic
routing, authentication and properties. The two source types can be enabled
independently. There is no alias or automatic fallback between them. A future
migration may change the `Ydb` alias only after existing sources are migrated;
this implementation does not perform that migration.

## Connection and authentication

Provide `LOCATION` as `host:port`, an absolute `DATABASE_NAME`, and `USE_TLS`
(`true` or `false`, default `false`). TLS uses certificate and hostname validation
against the driver's trusted roots. The initial endpoint is used directly;
endpoint discovery and database-ID resolution are not supported.

The direct path supports `AUTH_METHOD='TOKEN'` with `TOKEN_SECRET_PATH`, and
`AUTH_METHOD='NONE'`. The token principal must have permission to describe and
read the remote table. Tokens travel through secure parameters, not serialized
source settings. The only source-specific properties are `DATABASE_NAME` and
`USE_TLS`. DDL rejects missing or invalid connection settings, BASIC,
SERVICE_ACCOUNT and IAM authentication, `DATABASE_ID`, and topic properties
such as `SHARED_READING` and `SHARED_READING_GROUP`. The legacy `Ydb` contract is
unchanged. Database paths must be absolute and cannot contain empty, `.` or `..`
components. Hostname allowlists apply at DDL validation.

For an existing secret `remote_token`:

```sql
CREATE EXTERNAL DATA SOURCE remote_db WITH (
    SOURCE_TYPE = 'YdbExternal',
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
for both `YdbExternal` and legacy `Ydb` sources. Propagating the query deadline through the entire
remote read path is future work. Metadata loading has a separate finite local
budget.

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
The legacy `Ydb` Connector-based streamlookup path remains available under its
existing configuration; `YdbExternal` never falls back to it.

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
