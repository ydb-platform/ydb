LIBRARY()

SRCS(
    external_data_source.cpp
    external_source_factory.cpp
    object_storage.cpp
)

PEERDIR(
    library/cpp/scheme
    ydb/core/base
    ydb/core/protos
    ydb/library/yql/providers/s3/path_generator
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
