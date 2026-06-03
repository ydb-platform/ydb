GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    grpc_iam_service_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/testing/common
    ydb/public/api/client/yc_private/iam
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/iam_private
    ydb/public/sdk/cpp/src/client/types/core_facility
    ydb/public/sdk/cpp/src/library/issue
    ydb/public/sdk/cpp/src/library/time
)

END()
