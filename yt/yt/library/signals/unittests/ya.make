GTEST(unittester-library-signals)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    signal_registry_ut.cpp
    signal_blocking_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core
    yt/yt/library/backtrace_introspector
    yt/yt/library/signals
)

SIZE(SMALL)

END()
