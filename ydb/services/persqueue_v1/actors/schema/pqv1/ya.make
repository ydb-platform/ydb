LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/persqueue/public
    ydb/core/persqueue/public/schema
    ydb/library/persqueue/topic_parser
    ydb/public/api/protos
    ydb/services/lib/actors
    ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    add_consumer.cpp
    alter_topic.cpp
    common.cpp
    create_topic.cpp
    drop_topic.cpp
    remove_consumer.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
