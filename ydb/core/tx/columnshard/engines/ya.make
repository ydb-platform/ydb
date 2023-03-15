RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    column_engine_logs.cpp
    column_engine.cpp
    db_wrapper.cpp
    insert_table.cpp
    index_info.cpp
    indexed_read_data.cpp
    filter.cpp
    portion_info.cpp
    scalars.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat

    # for NYql::NUdf alloc stuff used in binary_json
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
