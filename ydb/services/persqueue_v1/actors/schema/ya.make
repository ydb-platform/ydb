LIBRARY()

PEERDIR(
    ydb/service/persqueue_v1/actors/schema/common
    ydb/service/persqueue_v1/actors/schema/pqv1
    ydb/service/persqueue_v1/actors/schema/topic
)

SRCS(
)

RECURSE(
    common
    pqv1
    topic
)

END()
