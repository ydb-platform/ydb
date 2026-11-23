LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    service.cpp
    controller_client.cpp
    store_initializer.cpp
    artifact_table_initializer.cpp
    kv_body_store.cpp
    table_query.cpp
    udf_name.cpp
    wasm_compile_actor.cpp
    wasm_library_compile_actor.cpp
    wasm_artifact_load_actor.cpp
    blob_chunks.cpp
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/json
    ydb/core/base
    ydb/core/keyvalue
    ydb/core/kqp/common/dynamic_function_registry
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/table_creator
    ydb/public/lib/udf/manifest
    ydb/services/metadata
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
    ydb/services/metadata/request
    ydb/services/udf_store/compile_controller/protos
    ydb/services/udf_store/metadata_subscription
    ydb/services/udf_store/wasm
    yql/essentials/minikql
)

END()

RECURSE(
    compile_controller
    wasm
)

RECURSE_FOR_TESTS(
    ut
    ut_orchestration
    ut_storage
)
