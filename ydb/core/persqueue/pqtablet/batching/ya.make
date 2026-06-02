LIBRARY()

SRCS(
    batch_processor.cpp
    consumer_batch_processor.cpp
)

PEERDIR(
    ydb/core/persqueue/common
    ydb/core/persqueue/events
)

END()
