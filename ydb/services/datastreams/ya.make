LIBRARY()

SRCS(
    datastreams_proxy.cpp
    grpc_service.cpp
    next_token.cpp
    put_records_actor.cpp
    shard_iterator.cpp
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/list_topics
    ydb/core/persqueue/public/partition_key_range
    ydb/core/persqueue/public/schema
    ydb/core/persqueue/public/write_meta
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tx/scheme_board
    ydb/library/actors/core
    ydb/library/cloud_permissions
    ydb/library/grpc/server
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/lib/base
    ydb/public/sdk/cpp/src/library/kafka
    ydb/services/datastreams/codes
    ydb/services/lib/actors
    ydb/services/lib/sharding
    ydb/services/persqueue_v1/actors
)

END()

RECURSE_FOR_TESTS(
    ut
)
