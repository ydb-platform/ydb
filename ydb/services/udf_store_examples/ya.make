# Example WASM UDFs for udf_store.
#
# Cross-compile (requires emscripten toolchain):
#   ya make --target-platform default-emscripten-wasm64 --build profile ydb/services/udf_store_examples/add
#
# Upload and query:
#   upload_udf --type WASM --udf-file <add.so> --manifest add/manifest.json ...
#   SELECT Add::add(10, 32);

RECURSE(
    add
)
