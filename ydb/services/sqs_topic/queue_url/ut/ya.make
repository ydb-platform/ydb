UNITTEST()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRCS(
    utils_ut.cpp
)

PEERDIR(
    ydb/services/sqs_topic/queue_url
)

END()
