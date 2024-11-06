UNITTEST_FOR(ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(SMALL)

TIMEOUT(60)

PEERDIR(
    ydb/core/testlib/pg
    ydb/library/yql/public/udf/service/exception_policy
    library/cpp/testing/unittest
)

SRCS(
    json_change_record_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
