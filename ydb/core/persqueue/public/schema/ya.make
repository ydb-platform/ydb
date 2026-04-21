LIBRARY()

SRCS(
    alter_topic.cpp
    alter_topic_internal.cpp
    alter_topic_operation.cpp
    common.cpp
    drop_topic.cpp
    drop_topic_operation.cpp
    remove_consumer_operation.cpp
    schema.cpp
    schema_operation.cpp
    validation.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/describer
)

END()

RECURSE_FOR_TESTS(
#    ut
)
