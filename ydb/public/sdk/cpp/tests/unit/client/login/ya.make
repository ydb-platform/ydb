UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    contrib/libs/grpc
    contrib/libs/jwt-cpp
    library/cpp/testing/common
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/common_client
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/types/credentials/login
)

SRCS(
    login_ut.cpp
)

END()
