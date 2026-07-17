# Example WASM UDFs for udf_store.
#
# Cross-compile with Arcadia/YT-compatible flags (required for WAVM):
#   ydb/services/udf_store/wasm/ya_make --target-platform=clang18-emscripten-wasm64 --build profile \
#     ydb/services/udf_store/wasm/sdk \
#     ydb/services/udf_store/wasm/protobuf \
#     ydb/services/udf_store_examples/add \
#     ydb/services/udf_store_examples/proto_simple
#
# Upload and query:
#   upload_udf --kind library --library-name sdk --udf-file <sdk.so> ...
#   upload_udf --kind library --library-name protobuf --udf-file <protobuf.so> ...
#   upload_udf --type WASM --udf-file <module.so> --manifest <manifest.json> ...
#   SELECT Add::add(10, 32);
#   SELECT ProtoSimple::proto_roundtrip(42);

RECURSE(
    add
    proto_simple
)
