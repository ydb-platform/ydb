UNITTEST_FOR(ydb/core/tx/columnshard/data_accessor)

SRCS(
    ut_manager.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/general_cache/usage
    ydb/library/yverify_stream
    ydb/core/tx/columnshard
    yql/essentials/sql/pg_dummy
    ydb/library/actors/core
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()