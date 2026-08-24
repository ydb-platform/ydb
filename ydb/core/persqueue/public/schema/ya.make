LIBRARY()

SRCS(
    add_consumer.cpp
    alter_topic.cpp
    alter_topic_internal.cpp
    alter_topic_operation.cpp
    check_dlq_topics.cpp
    common.cpp
    create_topic.cpp
    create_topic_internal.cpp
    create_topic_operation.cpp
    describe_operation.cpp
    drop_topic.cpp
    drop_topic_operation.cpp
    remove_consumer.cpp
    schema.cpp
    schema_operation.cpp
    schema_propose.cpp
    validation.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/persqueue/public/describer
    ydb/core/persqueue/public/nameresolver
    ydb/core/util
    ydb/core/ydb_convert
    ydb/library/aclib
)

END()

RECURSE_FOR_TESTS(
    ut
)
