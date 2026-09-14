LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    ../cpu_spec.cpp
    bridge_host.cpp
    bridge_node_table.cpp
    bridge_resident.cpp
    compartment_manager.cpp
    call_stack.cpp
    host.cpp
    manifest.cpp
    compile.cpp
    module_catalog.cpp
    registry_helpers.cpp
    single_module_loader.cpp
    system_libraries.cpp
    udf_configured_callable.cpp
    udf_function.cpp
)

ADDINCL(
    contrib/restricted/wavm_llvm16/Include
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
    ydb/library/wasm/api
    ydb/library/wasm/engine
    ydb/library/yql/dq/proto
    yql/essentials/public/udf
    yql/essentials/minikql
    library/cpp/json
    contrib/restricted/wavm_llvm16/Lib
)

END()

RECURSE(
    abi
    object_framework
)
