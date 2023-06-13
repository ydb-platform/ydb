UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_coordination)

IF (SANITIZER_TYPE)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/api/grpc
)

SRCS(
    coordination_ut.cpp
)

END()
