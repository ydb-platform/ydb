LIBRARY()

PEERDIR(
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/persqueue/public/schema
    ydb/core/ydb_convert
    ydb/library/persqueue/topic_parser
    ydb/public/api/protos
    ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    alter_topic.cpp
    create_topic.cpp
    describe_consumer.cpp
    describe_partition.cpp
    describe_topic.cpp
    drop_topic.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
