LIBRARY()
YQL_LAST_ABI_VERSION()

SRCS(
    async_runtime/host.cpp
    async_runtime/runtime.cpp
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
    contrib/restricted/wavm_llvm16/Lib
    library/cpp/json
    library/cpp/yt/assert
    ydb/library/wasm/api
    ydb/library/wasm/engine
    ydb/public/lib/udf/manifest
    ydb/public/lib/udf/service_manifest
    ydb/services/udf_store/wasm/abi
    yql/essentials/ast
    yql/essentials/minikql
    yql/essentials/public/decimal
    yql/essentials/public/issue
    yql/essentials/public/udf
)

END()

RECURSE(
    abi
    async_runtime/fixture
    object_framework
)
