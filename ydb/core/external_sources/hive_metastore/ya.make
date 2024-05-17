LIBRARY()

SRCS(
    hive_metastore_client.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/external_sources/hive_metastore/hive_metastore_native
)

END()

RECURSE_FOR_TESTS(
    ut
)
