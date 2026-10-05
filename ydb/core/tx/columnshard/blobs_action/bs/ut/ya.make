UNITTEST_FOR(ydb/core/tx/columnshard/blobs_action/bs)

SIZE(SMALL)

SRCS(
    ut_channel_selection.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/data_sharing/manager
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
