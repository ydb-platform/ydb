GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

EXPLICIT_DATA()

SRCS(
    helper.cpp

    counters_ut.cpp
    hedging_ut.cpp
    penalty_provider_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    library/cpp/iterator
    library/cpp/testing/common
    yt/yt/client/hedging
    yt/yt/client/unittests/mock
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/profiling/solomon
)

END()
