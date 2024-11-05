LIBRARY()

SRCS(
    events.cpp
    hive_metastore_client.cpp
    hive_metastore_converters.cpp
    hive_metastore_fetcher.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/external_sources/hive_metastore/hive_metastore_native
    ydb/library/actors/core
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/public/issue/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
