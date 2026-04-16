LIBRARY()

PEERDIR(
    ydb/services/persqueue_v1/actors/schema/common
    ydb/services/persqueue_v1/actors/schema/topic
)

SRCS(
)

END()

RECURSE(
    common
    topic
)
