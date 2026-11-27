BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

UNITTEST()

BUNDLE_OUTPUT(
    yql/essentials/public/sql_inspect/abi/fat
    sql_inspect_abi_internalized.a
    RENAME
    cached.a
)

SET_APPEND(LDFLAGS ${input:"cached.a"})

SRCS(
    yql/essentials/public/sql_inspect/symbols.cpp
    yql/essentials/public/sql_inspect/ut/symbols_ut.cpp
)

PEERDIR(
    yql/essentials/public/abi_impl
    yql/essentials/public/langver
    yql/essentials/public/sql_inspect/abi/interface
    yql/essentials/public/sql_inspect/abi/serialization
)

END()
