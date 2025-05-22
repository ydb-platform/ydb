UNITTEST_FOR(ydb/library/yql/providers/dq/actors)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/time_provider
    ydb/library/actors/testlib
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/dq/actors
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    grouped_issues_ut.cpp
    actors_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
