LIBRARY()

SRCS(
    queue_url_holder.cpp
)

PEERDIR(
    ydb/core/base
    ydb/services/sqs_topic/queue_url
)

END()
