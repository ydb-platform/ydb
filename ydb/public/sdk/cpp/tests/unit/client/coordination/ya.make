UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    util
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/coordination
)

SRCS(
    coordination_ut.cpp
    distributed_mutex_ut.cpp
)

END()
