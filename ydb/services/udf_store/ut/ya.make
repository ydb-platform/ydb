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
    yql/essentials/minikql/computation
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    manifest_ut.cpp
    blob_chunks_ut.cpp
    bridge_abi_ut.cpp
    bridge_dict_ut.cpp
    bridge_leak_ut.cpp
    bridge_node_table_ut.cpp
    compartment_manager_ut.cpp
    object_framework_ut.cpp
    objects_abi_ut.cpp
    shared_ctx_ut.cpp
    throw_exception_ut.cpp
    udf_name_ut.cpp
    with_helpers_ut.cpp
    ../wasm/manifest.cpp
    ../blob_chunks.cpp
    ../udf_name.cpp
)

RESOURCE(
    ydb/services/udf_store/ut/data/throw_with_dwarf.wasm /throw_with_dwarf.wasm
)

END()
