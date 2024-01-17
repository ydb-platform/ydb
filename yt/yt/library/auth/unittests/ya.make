GTEST(unittester-library-auth)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    auth_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/auth
)

SIZE(MEDIUM)

END()
