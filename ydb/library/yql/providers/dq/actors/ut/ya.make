UNITTEST_FOR(ydb/library/yql/providers/dq/actors)

TAG(ya:manual)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/time_provider
    ydb/library/actors/testlib
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    grouped_issues_ut.cpp
    actors_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
