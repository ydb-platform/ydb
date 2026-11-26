UNITTEST_FOR(ydb/services/udf_store/compile_controller)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/json
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common/dynamic_function_registry
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/services/metadata/abstract
    ydb/services/metadata/request
    ydb/services/udf_store
    ydb/services/udf_store/compile_controller/protos
    ydb/services/udf_store/metadata_subscription
    ydb/services/udf_store/wasm
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

SRCS(
    compile_controller_ut.cpp
    dinode_client_ut.cpp
    service_module_load_ut.cpp
    chunk_read_ut.cpp
)

END()
