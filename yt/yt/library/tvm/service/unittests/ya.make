GTEST(unittester-library-auth_tvm)

ALLOCATOR(YT)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build

    yt/yt/core/test_framework

    yt/yt/library/tvm/service
)

EXPLICIT_DATA()

IF(NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
