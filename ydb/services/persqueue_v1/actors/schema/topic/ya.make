LIBRARY()

PEERDIR(
    ydb/core/persqueue/public/schema
    ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    alter_topic.cpp
)

END()
