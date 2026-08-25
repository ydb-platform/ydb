UNITTEST()

PEERDIR(
    library/cpp/json
    library/cpp/resource
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/library/wasm/api
    ydb/library/wasm/engine
    ydb/services/udf_store/wasm
    ydb/services/udf_store/wasm/object_framework
)

SRCS(
    manifest_ut.cpp
    blob_chunks_ut.cpp
    compartment_manager_ut.cpp
    object_framework_ut.cpp
    objects_abi_ut.cpp
    shared_ctx_ut.cpp
    throw_exception_ut.cpp
    with_helpers_ut.cpp
    ../wasm/manifest.cpp
    ../blob_chunks.cpp
)

RESOURCE(
    ydb/services/udf_store/ut/data/throw_with_dwarf.wasm /throw_with_dwarf.wasm
)

END()
