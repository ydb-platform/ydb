LIBRARY()

PEERDIR(
    ydb/core/persqueue/public/schema
    ydb/services/persqueue_v1/actors/schema/common
    ydb/services/lib/actors
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
