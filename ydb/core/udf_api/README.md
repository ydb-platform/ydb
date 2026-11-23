# UDF service (P0/P1)

The experimental `Ydb.Udf.V1.UdfService` and `ydb experimental udf` manage WASM modules and libraries.
Build and invoke the experimental CLI binary at `ydb/apps/ydb/experimental/ydb/ydb`.
All commands require a cluster or database administrator and the endpoint serving that database.
This development API is updated in place; recreate test databases and regenerate clients.

```sh
ydb -e grpc://localhost:31011 -d /Root/test experimental udf upload --file libwasm-sdk.so --manifest sdk.manifest.json
ydb -e grpc://localhost:31011 -d /Root/test experimental udf upload --file libwasm-md5.so --manifest md5.manifest.json --create-only
ydb -e grpc://localhost:31011 -d /Root/test experimental udf upload --package md5.tar.gz
ydb -e grpc://localhost:31011 -d /Root/test experimental udf list --type library --kind wasm --format yaml
ydb -e grpc://localhost:31011 -d /Root/test experimental udf describe --name Md5 --format json
ydb -e grpc://localhost:31011 -d /Root/test experimental udf delete --name Md5 --expected-uid UPLOAD_UID
```

Every manifest must contain `module_name`, `module_type` (`module` or `library`),
and `module_kind` (`wasm` or `native`). Binary WASM defaults to `module_extension: wasm`;
textual modules and libraries require `wat` or `wast`. File suffixes have no effect.
For WASM modules, callable declarations follow the existing manifest schema.
Libraries must not include functions, objects, calling_convention or required_libraries.

Upload accepts `--write-mode create-or-replace|create-only|replace-only`,
`--create-only`, `--replace-only`, `--expected-uid`, and `--expected-md5`.
The shortcuts cannot be combined with each other or with `--write-mode`.
Every successful upload creates a new uid, including repeated identical content.
Native is recognized but returns PRECONDITION_FAILED; no native data is published.

As an alternative to `--file` plus `--manifest`, `--package` accepts ZIP, TAR,
TAR.GZ, or TGZ. The archive root must contain exactly `manifest.json` and one
other regular file, which is used as the module body. Directories, links,
duplicate names, and nested or additional files are rejected. The archive is
unpacked by the CLI; the RPC still receives manifest metadata and body chunks.

Output formats: upload text (default)/json; list table (default)/json/yaml;
describe json (default)/yaml. List follows all pages. JSON/YAML enum values are lowercase.
The C++ SDK also offers UploadModuleFromFile for bounded-memory file uploads.

For CI, poll describe, verify module.uid equals the returned upload uid, and require
nonempty platforms with every status ready. Fail on any failed platform, a changed uid,
or timeout. Compile state belongs to each platform artifact; READY requires a stored artifact for this uid.
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

A reusable CI polling example is in `ydb/tests/functional/udf_store/scripts/upload_and_wait.sh`.
Set `YDB_BIN`, `YDB_ENDPOINT` and `YDB_DATABASE`, and pass either `--file` plus
`--manifest` or `--package`.
