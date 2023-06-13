UNITTEST_FOR(ydb/services/bg_tasks)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
    ydb/services/bg_tasks
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_table
    ydb/services/bg_tasks/abstract
    ydb/services/bg_tasks
    library/cpp/testing/unittest
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
)

SRCS(
    ut_tasks.cpp
    GLOBAL task_emulator.cpp
)

SIZE(MEDIUM)
YQL_LAST_ABI_VERSION()

END()
