LIBRARY()

PEERDIR(
    ydb/services/persqueue_v1/actors/schema/common
    ydb/services/persqueue_v1/actors/schema/pqv1
    ydb/services/persqueue_v1/actors/schema/topic
)

SRCS(
)

END()

RECURSE(
    common
    pqv1
    topic
)
