GTEST(unittester-library-erasure)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    erasure_stability_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/library/erasure/impl
)

END()
