UNITTEST_FOR(ydb/library/yql/providers/dq/actors)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/time_provider
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/public/udf/service/stub
)

SRCS(
    grouped_issues_ut.cpp
)

END()
