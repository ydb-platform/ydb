LIBRARY()

PEERDIR(
    ydb/core/persqueue/public/schema
    ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    alter_topic.cpp
    common_describe.cpp
    create_topic.cpp
    describe_consumer.cpp
    describe_partition.cpp
    drop_topic.cpp
    partitions_location.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
