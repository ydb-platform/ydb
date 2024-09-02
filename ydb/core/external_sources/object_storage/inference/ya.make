LIBRARY()

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    ydb/library/yql/udfs/common/clickhouse/client/src
)

SRCS(
    arrow_fetcher.cpp
    arrow_inferencinator.cpp
    infer_config.cpp
)

PEERDIR(
    contrib/libs/apache/arrow

    ydb/core/external_sources/object_storage

    ydb/library/yql/providers/s3/compressors
)

END()

RECURSE_FOR_TESTS(
    ut
)
