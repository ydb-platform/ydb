IF (SANITIZER_TYPE AND AUTOCHECK)

ELSE()

UNITTEST_FOR(ydb/tools/query_replay_yt/lib)

PEERDIR(
    ydb/tools/query_replay_yt/lib
)

SRCS(
    plan_check_ut.cpp
)

SIZE(SMALL)

END()

UNITTEST_FOR(ydb/tools/query_replay_yt/lib)

PEERDIR(
    ydb/tools/query_replay_yt/lib
)

SRCS(
    replay_runner_ut.cpp
)

SIZE(LARGE)
TIMEOUT(300)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

END()

ENDIF()
