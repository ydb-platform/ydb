LIBRARY()

SRCS(
    builtins.cpp
    GLOBAL compartment.cpp
    GLOBAL data_transfer.cpp
    GLOBAL function.cpp
    intrinsics.cpp
    GLOBAL memory_pool.cpp
    GLOBAL system_libraries.cpp
    GLOBAL type_builder.cpp
)

ADDINCL(
    contrib/restricted/wavm_llvm16/Include
)

PEERDIR(
    ydb/library/wasm/api
    contrib/restricted/wavm_llvm16/Lib
    library/cpp/resource
    library/cpp/yt/assert
    library/cpp/yt/compact_containers
    library/cpp/yt/error
    library/cpp/yt/memory
    library/cpp/yt/misc
    util
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

END()
