LIBRARY()

SRCS(
    arrow_fetcher.cpp
    arrow_inferencinator.cpp
)

PEERDIR(
    contrib/libs/apache/arrow

    ydb/core/external_sources/object_storage
)

END()

RECURSE_FOR_TESTS(
    ut
)
