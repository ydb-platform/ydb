G_BENCHMARK()


PEERDIR(
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow
    ydb/core/formats/arrow/filter
    ydb/library/formats/arrow
    ydb/core/tx/columnshard/engines/changes/compaction
    ydb/core/tx/columnshard/engines/changes/compaction/abstract
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/engines/storage/indexes
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/data_sharing/manager
    ydb/core/tx/columnshard/data_accessor/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    contrib/libs/apache/arrow
    contrib/libs/apache/arrow_next
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SRCS(
    main.cpp
)

END()
