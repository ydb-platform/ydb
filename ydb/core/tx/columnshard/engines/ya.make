RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    column_engine_logs.cpp
    column_engine.cpp
    compaction_info.cpp
    db_wrapper.cpp
    index_info.cpp
    indexed_read_data.cpp
    index_logic_logs.cpp
    filter.cpp
    portion_info.cpp
    scalars.cpp
    tier_info.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/engines/reader
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/engines/storage
    ydb/core/tx/columnshard/engines/insert_table
    ydb/core/formats/arrow/compression
    ydb/core/tx/program

    # for NYql::NUdf alloc stuff used in binary_json
    ydb/library/yql/public/udf/service/exception_policy
)

GENERATE_ENUM_SERIALIZATION(portion_info.h)
YQL_LAST_ABI_VERSION()

END()
