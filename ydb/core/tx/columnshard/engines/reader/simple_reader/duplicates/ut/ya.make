UNITTEST_FOR(ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/blobs_action/counters
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/data_accessor/abstract
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/tx/columnshard/data_sharing/manager
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/engines/storage/indexes
    ydb/core/tx/columnshard/splitter
    ydb/library/actors/core
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_filters.cpp
)

END()
