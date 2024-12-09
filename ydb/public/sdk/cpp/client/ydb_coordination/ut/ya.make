UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_coordination)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
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
