LIBRARY()

ADDINCL(
    ydb/library/yql/udfs/common/clickhouse/client/base
    ydb/library/yql/udfs/common/clickhouse/client/src
)

SRCS(
    arrow_fetcher.cpp
    arrow_inferencinator.cpp
)

PEERDIR(
    contrib/libs/apache/arrow

    ydb/core/external_sources/object_storage

    ydb/library/yql/udfs/common/clickhouse/client
)

END()

RECURSE_FOR_TESTS(
    ut
)
