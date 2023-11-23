GTEST()

SRCS(
    introspect_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/library/backtrace_introspector

    yt/yt/core/test_framework
)

END()
