LIBRARY()

PEERDIR(
    ydb/core/persqueue/public/schema
    ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    add_consumer.cpp
    alter_topic.cpp
    common.cpp
    drop_topic.cpp
)

END()
