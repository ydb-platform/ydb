# UDF manifest

`NYdb::NUdfManifest::Parse` validates the common JSON contract without depending
on server code. Every manifest requires nonempty string `module_name`,
`module_type` (`module` or `library`) and `module_kind` (`wasm` or `native`).
Classification values are case-sensitive. Native classification is reserved;
this library does not implement native upload or loading.

WASM modules and libraries may specify `module_extension`: `wasm` (default),
`wat` or `wast`. The manifest determines the format, not the local filename.
`functions`, `objects`, `calling_convention` and `required_libraries` apply only
to WASM modules. Inapplicable fields are rejected even when empty.

The runtime WASM parser additionally validates callable declarations. The library
compiler reads the saved manifest and verifies its name and classification.
Library manifests are stored in `modules.manifest`, just like module manifests.

Minimal binary library manifest:

```json
{"module_name":"sdk","module_type":"library","module_kind":"wasm"}
```

The test uploader accepts the manifest for both WASM modules and libraries:

```sh
upload_udf --endpoint grpc://localhost:31011 --database /Root/test \
  --type WASM --kind library --udf-file libwasm-sdk.so --manifest manifest.json
```

This development schema replaces the old manifests directly. Recreate test
databases and reload the updated fixtures; no data migration is included.
