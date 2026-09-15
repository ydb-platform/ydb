LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    service.cpp
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
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/keyvalue
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/table_creator
    ydb/services/udf_store/metadata_subscription
    ydb/services/udf_store/wasm
    ydb/services/metadata/request
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
    ydb/services/metadata
    yql/essentials/minikql
    library/cpp/digest/md5
    library/cpp/json
)

END()

RECURSE(
    wasm
)

RECURSE_FOR_TESTS(
    ut
)
