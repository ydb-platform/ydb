GTEST(unittester-client-queue-client)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    cross_cluster_reference_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
