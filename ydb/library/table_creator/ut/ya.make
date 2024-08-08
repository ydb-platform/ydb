UNITTEST_FOR(ydb/library/table_creator)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    table_creator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/public/sdk/cpp/client/ydb_driver
)

YQL_LAST_ABI_VERSION()

END()
