YQL_UDF(wasm_udf)

YQL_ABI_VERSION(
    2
    38
    0
)

SUBSCRIBER(g:yql)

SRCS(
    module.cpp
    udf_init.cpp
    udf_load.cpp
    wasm_describe.cpp
    wasm_directory_state.cpp
    wasm_invoke.cpp
    wasm_run.cpp
    wasm_signature.cpp
    wasm_state.cpp
    wasm_system_libraries.cpp
    wasm_udf_function.cpp
    wasm_udf_host.cpp
    wasm_udf_registry.cpp
    wasm_udf_registry_helpers.cpp
)

ADDINCL(
    contrib/restricted/wavm/Include
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

PEERDIR(
    yql/essentials/udfs/common/wasm/abi
    ydb/library/wasm/api
    ydb/library/wasm/engine
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    test
)
