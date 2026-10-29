BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

UNITTEST()

BUNDLE_OUTPUT(
    yql/essentials/public/sql_format/abi/fat
    sql_format_abi_internalized.a
    RENAME
    cached.a
)

SET_APPEND(LDFLAGS ${input:"cached.a"})

SRCS(
    yql/essentials/public/sql_format/sql_format.cpp
    yql/essentials/public/sql_format/ut/sql_format_ut.cpp
)

PEERDIR(
    yql/essentials/public/sql_format/abi/interface
)

END()
