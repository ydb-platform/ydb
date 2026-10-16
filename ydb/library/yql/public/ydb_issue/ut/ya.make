UNITTEST_FOR(ydb/library/yql/public/ydb_issue)

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
SRCS(
    ydb_issue_ut.cpp
)

END()
