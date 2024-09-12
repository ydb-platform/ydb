LIBRARY()

SRCS(
    arrow_fetcher.cpp
    arrow_inferencinator.cpp
)

PEERDIR(
    contrib/libs/apache/arrow

    ydb/core/external_sources/object_storage

    ydb/library/services
    ydb/library/yql/providers/s3/compressors
)

END()

RECURSE_FOR_TESTS(
    ut
)
