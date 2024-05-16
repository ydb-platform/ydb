GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cache_ut.cpp
    options_ut.cpp
    rpc_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    library/cpp/testing/common
    yt/yt/client/cache
)

END()
