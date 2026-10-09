# Experimental FQ WASM service integration

This directory contains service-independent native HTTP/gRPC clients and an
analytical FQ/DQ output transform. Concrete adapters live in `ydb/udfs/wasm`:
`profile` owns its JSON/protobuf contracts and decoding; `echo` is a second
module with different input/output schemas and two methods. Adding a module
does not require modifying this transform or the coroutine runtime.

## Registration and SQL

Install operator-owned modules and matching manifests on all compiling and
executing FQ nodes. The manifest extends the existing common WASM manifest
fields with the experimental `service_methods` contract described in
`ydb/udfs/wasm/sdk/services/README.md`. Module names and method IDs/types are
discovered from these files, not a native Profile registry. Authors edit a
module's deployable `manifest.json`, including the ABI version. The build
validates it and generates only guest dispatch constants. FQ reads this same
author-owned file directly; there is no generated manifest or method `id` field.

```protobuf
WasmServices {
  Enabled: true
  Modules {
    ModulePath: "/absolute/path/profile.wasm"
    ManifestPath: "/absolute/path/profile/manifest.json"
  }
  Modules {
    ModulePath: "/absolute/path/echo.wasm"
    ManifestPath: "/absolute/path/echo/manifest.json"
  }
  MaxBatchRows: 2
  MaxBatchBytes: 32768
  MaxBufferedRows: 65536
  MaxBufferedBytes: 67108864
  CallTimeoutMs: 30000
  Bindings {
    Alias: "profiles_http"
    Protocol: HTTP
    Endpoint: "https://profiles.example.test/profile/batch"
    CaFile: "/absolute/path/ca.pem"
    Headers { key: "Content-Type" value: "application/json" }
    Headers { key: "Authorization" value: "Bearer operator-owned-token" }
  }
  Bindings {
    Alias: "echo_http"
    Protocol: HTTP
    Endpoint: "http://127.0.0.1:8080/echo"
    Headers { key: "Content-Type" value: "application/octet-stream" }
  }
}
```

`ModulePath` is retained as a single-module configuration shorthand: it requires
an adjacent `<ModulePath>.manifest.json`. Do not combine it with `Modules`.
Rebuild old modules: service row ABI version 2 derives dispatch IDs from method
names instead of manual manifest IDs. Version 1 manifests/requests are rejected.
The underlying async ABI is unchanged. Profile's SQL spelling is preserved.

```sql
$input = SELECT 42ul AS id UNION ALL SELECT 43ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
    WITH CONNECTION='profiles_http',
         INPUT_TYPE=Struct<id:Uint64>,
         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT id, name, score FROM $profiles;
```

For another module, select its manifest name/method and declared schemas, e.g.
`EXTERNAL FUNCTION('Echo', 'Echo')`; see `ydb/udfs/wasm/echo/query.sql`.
Unknown modules/methods/aliases, mismatched row schemas and invalid manifests
fail registration/query resolution before network dispatch.
Execution byte/batch limits are checked for the selected method, so an unused
method with a larger declared row does not disable unrelated modules.

## Ownership, Batching and Limits

Each transform owns a compartment, async runtime and native transport. All
guest entries happen on its actor mailbox; native completions only wake it.
Pending I/O frees the calling thread. Query teardown cancels calls and drains
physical native I/O. Endpoints, CA files, methods and credentials stay on the
host; plans contain module/method/alias identities and guests receive only a
binding index, protocol and bounded row bytes.

`MaxBatchRows` zero/one selects scalar mode; 2..64 requests batching. Selected
methods must declare batch support. Cardinality is capped by the method limit,
its maximum input/output row sizes and `MaxBatchBytes` (default 32768, hard cap
32768). The byte limit must fit a request/result header plus one declared row.
For Profile the minimum remains 304 bytes. No timer waits for a full batch;
DQ partitioning/input delivery can produce smaller batches, including one row.

The module owns network payload construction, decoding, domain validation and
correlation. The host validates version, exact cardinality, all field bounds,
UTF-8, booleans and complete result framing before publishing any batch row.
There is one active batch per transform. Deadlines/cancellation cover the
whole call; errors fail the query without publishing partial batch results.
Backpressure retains results and prevents further dispatch. Output row order
follows the module contract; overall SQL order still requires `ORDER BY`.

Buffered input, active rows and undelivered results count toward row quotas.
`MaxBufferedBytes` conservatively reserves the larger declared input/output
row size for each queued row (default 64 MiB). These are serialized-buffer
reservations, not an RSS limit for containers or curl/gRPC/TLS internals.
Native transport additionally reserves request/response copies and releases
leases only after physical request handles/buffers are destroyed.

The clients use one curl multi worker for HTTP/timers and one gRPC completion
queue worker. TLS certificates/hostnames are checked; optional CA files are
operator-owned. Plaintext gRPC requires explicit `GrpcInsecure: true`.
Redirects, ambient proxies and application/gRPC retries are disabled.
Diagnostics contain fixed text and numeric error classes, never service
payloads, endpoints or credentials. The production HTTP gateway choice remains
open; this prototype uses curl without changing shared HTTP APIs.

## Verification and Remaining Gates

```bash
./ya make --build relwithdebinfo -tA ydb/core/fq/libs/wasm_services/ut
./ya make --build relwithdebinfo -tA ydb/tests/fq/wasm_services \
    --test-env=YDB_DRIVER_BINARY=/absolute/path/to/installation/ydbd
```

Tests start isolated FQ nodes and local HTTP/gRPC mocks. They leave the normal
installation's config/storage unchanged. The installed binary must point to
the current build. Example-module build/regeneration commands are documented
in `ydb/udfs/wasm/profile/README.md` and `ydb/udfs/wasm/echo/README.md`.

This remains opt-in analytical FQ/DQ v1, not KQP/YQv2, synchronous robust UDF
calls or the production WASM object ABI. Streaming/checkpoints, nested/optional
row types, Arrow, parallel batches, automatic retries and replay guarantees
are not implemented. Metadata ACLs, credential refresh, module identity/version
pinning, connection-aware pooling and production tenant resource accounting
remain separate gates. Enabling the prototype grants analytical query users
access to configured aliases; keep it disabled on shared production tenants.
