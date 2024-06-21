LIBRARY()

ADDINCL(
    contrib/libs/poco/Foundation/include
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/public/issue
    ydb/library/yql/udfs/common/clickhouse/client
)

IF (CLANG AND NOT WITH_VALGRIND)

    CFLAGS (
        -DARCADIA_BUILD -DUSE_PARQUET
    )

    SRCS(
        events.cpp
    )

ENDIF()

END()
