RECURSE(
    object_storage
)

LIBRARY()

SRCS(
    external_data_source.cpp
    external_source_factory.cpp
    object_storage.cpp
    validation_functions.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/external_sources/object_storage/inference
    ydb/library/actors/http
    ydb/library/yql/providers/common/gateway
    library/cpp/regex/pcre
    library/cpp/scheme
    ydb/core/base
    ydb/core/protos
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/s3/common
    ydb/library/yql/providers/s3/object_listers
    ydb/library/yql/providers/s3/path_generator
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/public/issue
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    hive_metastore
)
