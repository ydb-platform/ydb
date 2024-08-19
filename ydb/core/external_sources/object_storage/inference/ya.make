LIBRARY()

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

IF (CLANG AND NOT WITH_VALGRIND)

    SRCS(
        arrow_fetcher.cpp
        arrow_inferencinator.cpp
    )

    PEERDIR(
        contrib/libs/apache/arrow

        ydb/core/external_sources/object_storage

        ydb/library/yql/providers/s3/compressors
    )

ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
