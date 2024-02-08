LIBRARY()

SRCS(
    external_data_source.cpp
    external_source_factory.cpp
    object_storage.cpp
    validation_functions.cpp
)

PEERDIR(
    ydb/library/actors/http
    library/cpp/regex/pcre
    library/cpp/scheme
    ydb/core/base
    ydb/core/protos
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/s3/path_generator
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
