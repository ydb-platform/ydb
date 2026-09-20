IF (NOT OS_WINDOWS)

UNITTEST_FOR(ydb/library/yql/providers/s3/actors)

SRCS(
    yql_arrow_push_down_ut.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        yql_arrow_column_converters_ut.cpp
    )

    PEERDIR(
        ydb/library/yql/udfs/common/clickhouse/client
    )

    ADDINCL(
        ydb/library/yql/udfs/common/clickhouse/client/base
        ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
        ydb/library/yql/udfs/common/clickhouse/client/src
    )

ENDIF()

YQL_LAST_ABI_VERSION()

END()

ENDIF()
