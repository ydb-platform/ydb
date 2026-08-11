GTEST()

SRCS(
    wasm_ut.cpp
)

IF (OPENSOURCE)
    SRCS(
        disable_system_libraries.cpp
    )
ELSE()
    SRCS(
        enable_system_libraries.cpp
    )
ENDIF()

# Required for linking code of bc functions.
LDFLAGS(-rdynamic)

ADDINCL(
    contrib/restricted/wavm_llvm16/Include
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

PEERDIR(
    ydb/library/wasm/api
    ydb/library/wasm/engine
    library/cpp/testing/gtest
)

SIZE(MEDIUM)

END()
