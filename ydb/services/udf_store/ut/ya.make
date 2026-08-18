UNITTEST()
YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/json
    library/cpp/resource
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/library/wasm/api
    ydb/library/wasm/engine
    ydb/services/udf_store/wasm
    ydb/services/udf_store/wasm/object_framework
    yql/essentials/minikql
    # DeleteString: the string release entry point emitted by MiniKQL codegen.
    yql/essentials/minikql/computation/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    manifest_ut.cpp
    blob_chunks_ut.cpp
    compartment_manager_ut.cpp
    object_framework_ut.cpp
    objects_abi_ut.cpp
    prefer_wasm_string_ut.cpp
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
