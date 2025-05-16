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
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/s3/events
    yql/essentials/core
    yql/essentials/minikql/dom
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
    yql/essentials/ast
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
