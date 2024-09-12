LIBRARY()

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_arrow_push_down.cpp
    yql_s3_actors_factory_impl.cpp
    yql_s3_actors_util.cpp
    yql_s3_applicator_actor.cpp
    yql_s3_raw_read_actor.cpp
    yql_s3_write_actor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    contrib/libs/poco/Util
    ydb/library/actors/http
    library/cpp/protobuf/util
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/xml/document
    ydb/core/base
    ydb/core/fq/libs/events
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/arrow
    ydb/library/yql/providers/common/arrow/interface
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/generic/pushdown
    ydb/library/yql/providers/s3/actors_factory
    ydb/library/yql/providers/s3/common
    ydb/library/yql/providers/s3/compressors
    ydb/library/yql/providers/s3/credentials
    ydb/library/yql/providers/s3/events
    ydb/library/yql/providers/s3/object_listers
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/providers/s3/range_helpers
    ydb/library/yql/public/issue
    ydb/library/yql/public/types
    ydb/library/yql/udfs/common/clickhouse/client
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        yql_arrow_column_converters.cpp
        yql_s3_decompressor_actor.cpp
        yql_s3_read_actor.cpp
        yql_s3_source_queue.cpp
    )

    PEERDIR(
        ydb/library/yql/providers/s3/range_helpers
        ydb/library/yql/providers/s3/serializations
    )

    CFLAGS (
        -DARCADIA_BUILD -DUSE_PARQUET
    )

ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
