BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

UNITTEST()

BUNDLE_OUTPUT(
    yql/essentials/public/fastcheck/abi/fat
    fastcheck_abi_internalized.a
    RENAME
    cached.a
)

SET_APPEND(LDFLAGS ${input:"cached.a"})

SRCS(
    yql/essentials/public/fastcheck/fastcheck.cpp
    yql/essentials/public/fastcheck/linter.cpp
    yql/essentials/public/fastcheck/parse_checks.cpp
    yql/essentials/public/fastcheck/ut/fastcheck_ut.cpp
    yql/essentials/public/fastcheck/ut/linter_ut.cpp
    yql/essentials/public/fastcheck/ut/linter_udf_ut.cpp
    yql/essentials/public/fastcheck/ut/public_ut.cpp
)

PEERDIR(
    yql/essentials/public/fastcheck/abi/interface
    yql/essentials/public/fastcheck/abi/serialization
    yql/essentials/public/udf_meta/interface
)

GENERATE_ENUM_SERIALIZATION(yql/essentials/public/fastcheck/linter.h)

END()
