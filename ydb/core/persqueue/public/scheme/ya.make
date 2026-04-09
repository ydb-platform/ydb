LIBRARY()

SRCS(
    alter_topic.cpp
    scheme.cpp
    scheme_int.cpp
    topic_alterer.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/describer
)

END()

RECURSE_FOR_TESTS(
    ut
)
