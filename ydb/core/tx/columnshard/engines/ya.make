RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    column_engine_logs.cpp
    column_engine.cpp
    column_features.cpp
    db_wrapper.cpp
    index_info.cpp
    filter.cpp
    portion_info.cpp
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
    ydb/core/tx/columnshard/engines/changes
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/protos
    ydb/core/tx/program
    ydb/core/tx/columnshard/common

    # for NYql::NUdf alloc stuff used in binary_json
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
