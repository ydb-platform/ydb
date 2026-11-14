UNITTEST_FOR(ydb/core/tx/columnshard/blobs_reader)

SIZE(SMALL)

SRCS(
    ut_retry.cpp
    ut_glue.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/actors/testlib
    ydb/library/services
    ydb/library/signals
    ydb/core/tx/columnshard/blobs_action/counters
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/resource_subscriber
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
