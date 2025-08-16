RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    metadata_accessor.cpp
    column_engine_logs.cpp
    column_engine.cpp
    db_wrapper.cpp
    filter.cpp
    defs.cpp
)

GENERATE_ENUM_SERIALIZATION(column_engine_logs.h)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/engines/changes
    ydb/core/tx/columnshard/engines/loading
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/engines/protos
    ydb/core/tx/columnshard/engines/reader
    ydb/core/tx/columnshard/engines/storage
    ydb/core/tx/columnshard/tracing
    ydb/core/tx/program

    # for NYql::NUdf alloc stuff used in binary_json
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
