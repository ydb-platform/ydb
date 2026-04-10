LIBRARY()

SRCS(
    alter_topic.cpp
    common.cpp
    scheme.cpp
    topic_alterer.cpp
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
