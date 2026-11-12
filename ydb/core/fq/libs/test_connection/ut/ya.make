UNITTEST_FOR(ydb/core/fq/libs/test_connection)

PEERDIR(
    library/cpp/logger
    library/cpp/testing/unittest
    ydb/core/testlib
    ydb/core/testlib/actors
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_connection_ut.cpp
)

END()
