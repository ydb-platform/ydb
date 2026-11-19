UNITTEST_FOR(ydb/public/sdk/cpp/src/client/topic)

REQUIREMENTS(ram:32 cpu:2)

IF (CLANG_COVERAGE AND OS_LINUX)
    # Large coverage binaries can exceed the DWARF32 section-size limit.
    LDFLAGS(-Wl,--strip-debug)
ENDIF()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    txusage_slow_ut.cpp
)

END()
