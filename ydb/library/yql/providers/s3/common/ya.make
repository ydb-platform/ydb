LIBRARY()

ADDINCL(
    contrib/libs/poco/Foundation/include
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

SRCS(
    util.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/s3/events
    ydb/library/yql/public/issue
    ydb/library/yql/public/issue/protos
)

IF (CLANG AND NOT WITH_VALGRIND)

    CFLAGS (
        -DARCADIA_BUILD -DUSE_PARQUET
    )

    SRCS(
        source_context.cpp
    )

ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
