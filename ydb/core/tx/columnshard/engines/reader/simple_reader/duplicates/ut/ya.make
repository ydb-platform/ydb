UNITTEST_FOR(ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/blobs_action/counters
    ydb/core/tx/columnshard/column_fetching
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/data_accessor
    ydb/core/tx/columnshard/data_accessor/abstract
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/tx/columnshard/data_sharing/manager
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/engines/reader/common
    ydb/core/tx/columnshard/engines/reader/common_reader/common
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/columnshard/engines/reader/simple_reader/constructor
    ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates
    ydb/core/tx/columnshard/engines/reader/simple_reader/iterator
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/engines/storage/indexes
    ydb/core/tx/columnshard/splitter
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/conveyor_composite/usage
    ydb/core/tx/general_cache/usage
    ydb/library/actors/core
    ydb/library/actors/testlib
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_filters.cpp
    ut_manager.cpp
)

END()
