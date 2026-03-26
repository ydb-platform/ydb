UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm)

SRCS(
    ut_bloom_ngramm.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy

    # These should not be here, but adding them to proper location creates dependency loops
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/data_accessor/abstract
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
    ydb/core/tx/columnshard/engines/storage/indexes/max
    ydb/core/tx/columnshard/engines/storage/indexes/portions
    ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    ydb/core/tx/columnshard/data_sharing/manager
)

END()
