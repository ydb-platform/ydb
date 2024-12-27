GTEST(unittester-client-signature)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    signature_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client
)

SIZE(SMALL)

END()
