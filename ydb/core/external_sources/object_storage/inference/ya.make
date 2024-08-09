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

<<<<<<< HEAD
    ydb/library/yql/providers/s3/compressors
=======
>>>>>>> 13f93591fe8eba5a84f704629ab20af22ecb2743
    ydb/library/yql/udfs/common/clickhouse/client
)

END()

RECURSE_FOR_TESTS(
    ut
)
