# UDF service (P0/P1)

The experimental `Ydb.Udf.V1.UdfService` manages WASM modules and libraries.
All RPC methods require a cluster or database administrator and the endpoint serving that database.
This development API is updated in place; recreate test databases and regenerate clients.

Every manifest must contain `module_name`, `module_type` (`module` or `library`),
and `module_kind` (`wasm` or `native`). Binary WASM defaults to `module_extension: wasm`;
textual modules and libraries require `wat` or `wast`. File suffixes have no effect.
For WASM modules, callable declarations follow the existing manifest schema.
Libraries must not include functions, objects, calling_convention or required_libraries.

Upload accepts `write_mode` (CREATE_OR_REPLACE, CREATE_ONLY, REPLACE_ONLY),
`expected_uid`, and `expected_md5` in UploadModuleParams.
Every successful upload creates a new uid, including repeated identical content.
Native is recognized but returns PRECONDITION_FAILED; no native data is published.

For CI, poll describe, verify module.uid equals the returned upload uid, and require
nonempty platforms with every status ready. Fail on any failed platform, a changed uid,
or timeout. Global compile_status is not a readiness guarantee. Platforms combine
artifact tables and the compile controller; READY requires a stored artifact for this uid.
This indicates compiled code, not that every node has loaded it. There is no server-side wait.

Errors are YDB Operation statuses; handled application errors use gRPC OK:

| Condition | Operation status |
| --- | --- |
| Invalid manifest/body/stream/enum, wrong tenant | BAD_REQUEST |
| Native, disabled WASM store, classification/checksum mismatch | PRECONDITION_FAILED |
| CREATE_ONLY on an existing name | ALREADY_EXISTS |
| REPLACE_ONLY/describe/delete of a missing name | NOT_FOUND |
| Stale expected_uid or conflicting transaction | ABORTED |
| Caller is not an administrator | UNAUTHORIZED |
| Cannot read complete platform state | UNAVAILABLE (or the underlying read error) |
| Describe deadline | TIMEOUT |

Upload enforces the existing fixed body cap. Tenant quotas, SQL+EDS and native
implementation are outside P0/P1.
