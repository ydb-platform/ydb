UNITTEST_FOR(ydb/library/yql/core/issue)

FORK_SUBTESTS()

SRCS(
    yql_issue_ut.cpp
)

PEERDIR(
)

TIMEOUT(300)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

END()
