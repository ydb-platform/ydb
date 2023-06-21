LIBRARY()

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_s3_actors_util.cpp
    yql_s3_applicator_actor.cpp
    yql_s3_sink_factory.cpp
    yql_s3_source_factory.cpp
    yql_s3_write_actor.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/poco/Util
    library/cpp/actors/http
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/xml/document
    ydb/core/fq/libs/events
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/public/types
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/arrow
    ydb/library/yql/providers/common/arrow/interface
    ydb/library/yql/providers/s3/common
    ydb/library/yql/providers/s3/compressors
    ydb/library/yql/providers/s3/object_listers
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/udfs/common/clickhouse/client
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        yql_s3_read_actor.cpp
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
