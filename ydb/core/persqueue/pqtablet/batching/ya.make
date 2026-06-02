LIBRARY()

SRCS(
    batch_processor.cpp
    batch_cutter.cpp
    consumer_batch_processor.cpp
)

PEERDIR(
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/core/persqueue/public/kafka_batch
)

END()
