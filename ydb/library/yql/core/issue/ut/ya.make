UNITTEST_FOR(ydb/library/yql/core/issue)

TAG(ya:manual)

FORK_SUBTESTS()

SRCS(
    yql_issue_ut.cpp
)

PEERDIR(
)

TIMEOUT(300)
SIZE(MEDIUM)

END()
