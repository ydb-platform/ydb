# YDB provider: direct remote reads

The experimental `EnableNativeYdbProvider` feature flag routes reads from
`SOURCE_TYPE='Ydb'` external data sources directly to the remote YDB Query Service.
The internal provider name is `ydb_remote`. The flag defaults to disabled; with
it disabled, YDB reads keep using Connector. Enabling it changes routing for all
YDB external data sources on the consumer, without automatic fallback.

## Connection and authentication

Provide `LOCATION` as `host:port`, an absolute `DATABASE_NAME`, and `USE_TLS`
(`true` or `false`, default `false`). TLS uses certificate and hostname validation
against the driver's trusted roots. The initial endpoint is used directly;
endpoint discovery and database-ID resolution are not supported.

The direct path supports `AUTH_METHOD='TOKEN'` with `TOKEN_SECRET_PATH`, and
`AUTH_METHOD='NONE'`. The token principal must have permission to describe and
read the remote table. Tokens travel through secure parameters, not serialized
source settings. BASIC, SERVICE_ACCOUNT, IAM and `DATABASE_ID` configurations
accepted by existing external-source DDL fail when compiled for the direct path.

For an existing secret `remote_token`:

```sql
CREATE EXTERNAL DATA SOURCE remote_db WITH (
    SOURCE_TYPE = 'Ydb',
    LOCATION = 'remote.example:2135',
    DATABASE_NAME = '/Remote',
    AUTH_METHOD = 'TOKEN',
    TOKEN_SECRET_PATH = 'remote_token',
    USE_TLS = 'true',
    READ_TIMEOUT_MS = '120000'
);

SELECT Key FROM remote_db.`items`;
```

`READ_TIMEOUT_MS` is an integer from 1 through 3600000. Its explicit default is
60000 ms. It bounds the entire source read, including retries and time waiting
for a slow downstream consumer; it is not an inactivity timeout. Increasing the
query or script timeout alone does not increase this source timeout. The property
is accepted by external-source DDL regardless of the routing flag, but only the
direct path applies it. Metadata loading has a separate finite local budget.

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
physical projection as `ReadColumns` and the source timeout as `ReadTimeoutMs`.
Filters, limits, aggregation and joins execute locally; they are not pushed into
the remote query.

One source uses one Query Service stream and one snapshot; reads are not split
into parallel partitions. Separate source reads do not share a remote snapshot.
Retries are limited to transient failures before any rows are delivered. Writing
to the remote external source, streamlookup joins and reads from
`CREATE/ALTER STREAMING QUERY` are unsupported.
In particular, enabling the flag also disables the Connector-based streamlookup
enrichment path for these data sources; no fallback is attempted.

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
