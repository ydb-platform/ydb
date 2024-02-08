GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

EXPLICIT_DATA()

SRCS(
    counters_ut.cpp
    hedging_ut.cpp
    penalty_provider_ut.cpp

    GLOBAL hook.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    library/cpp/iterator
    library/cpp/testing/common
    library/cpp/testing/hook
    yt/yt/client/hedging
    yt/yt/client/unittests/mock
    yt/yt/core
    yt/yt/library/profiling/solomon
)

END()
