# Experimental asynchronous service-module SDK

`rows.h` defines a service-independent, little-endian row contract. It is a
private experimental extension over P1's existing coroutine/async ABI, not a
replacement for the stable bridge/object UDF ABI. `transport.h` defines the
opaque network request/response envelope; the native owner resolves binding
indices into trusted HTTP/gRPC configuration.

## Module Manifest

Keep the usual `module_type: module`, `module_kind: wasm`, `module_name` and
`module_extension: wasm` fields. Add `service_abi_version: 1` and a nonempty
`service_methods` array. Each method declares:

- `name` and unique uint32 `id` (the guest dispatch ID).
- `batch`, `max_batch_rows` (1..64; non-batch methods must use 1).
- Ordered `input` and `output` field lists with unique `name` and `type`.
- `max_bytes` for String/Utf8 fields, a positive hard byte bound.
- `max_output_row_bytes`, at least the complete maximum serialized output row.

Names contain ASCII letters/digits/underscore, up to 128 characters. Supported
non-null scalar types are Uint64, Uint32, Int64, Bool, String and Utf8. There
are at most 32 fields per row and 64 methods per manifest. Row sizes must fit
the hard 32768-byte request/result cap including headers. Optional/nested types
and Arrow are explicitly unsupported rather than silently reinterpreted.
See the Profile and Echo manifests for examples.

## Row and Call Layout

`TServiceRequest` is followed by `Count` rows. It carries a magic/version,
method ID, binding index, HTTP=0/gRPC=1, count, byte cap and batch flag. Field
order is the manifest order; the host maps SQL struct members by name/type.
Integers use fixed-width little-endian bytes, Bool uses uint8 0/1, and
String/Utf8 use uint32 byte length followed by bytes. Embedded zero bytes and
empty strings are valid at this boundary; the adapter may impose domain rules.

Return `TServiceResult` followed by exactly one output row per input position.
On failure return a numeric `Error` and `Detail`, not a backend error string.
Successful results must have matching version/count, zero error/detail and no
trailing bytes. Host framing/type/UTF-8/size checks validate the whole batch
before any row becomes visible. Correlation and domain constraints belong to
the guest: Profile checks IDs; Echo checks echoed binary strings.

Only the selected method ID/binding index and row data enter the guest.
Metadata, auth headers, endpoints and certificates remain native. Deploy the
module and its matching manifest together on all nodes; artifact identity
pinning and dynamic metadata registration remain future integration work.

## Implementing Another Adapter

Use the P1 `TTask`, `TCallContext` and `TOperation` for nonblocking network calls.
`module.h` supplies result-buffer ownership and `WASM_SERVICE_MODULE(handler)`
for the P1 exports. Echo demonstrates two method IDs with different result
schemas. `example_allocator.h` is bounded example storage with lifetime
counters, not a required allocator ABI or production allocator.

The prototype uses the registry's minimal runtime, not the full C/C++ SDK.
Link any additional guest libc functions into the adapter itself, as Echo does
for Emscripten's `memcmp`; do not introduce module-specific host intrinsics.

Place new adapters under `ydb/udfs/wasm/<module>`, build with the existing WASM
toolchain, and register the artifact/manifest in `WasmServices.Modules`.
No native per-module registry, serializer or decoder should be added to FQ.
