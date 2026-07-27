LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    ../cpu_spec.cpp
    host.cpp
    manifest.cpp
    compile.cpp
    registry_helpers.cpp
    single_module_loader.cpp
    system_libraries.cpp
    udf_function.cpp
)

ADDINCL(
    contrib/restricted/wavm/Include
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
    ydb/library/wasm/api
    ydb/library/wasm/engine
    yql/essentials/public/udf
    yql/essentials/minikql
    library/cpp/json
    contrib/restricted/wavm/Lib
)

END()

RECURSE(
    abi
)
