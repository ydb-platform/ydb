LIBRARY()

SRCS(
    batch_processor.cpp
    batch_cutter.cpp
    consumer_batch_processor.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/library/kafka
    ydb/core/persqueue/public/write_meta
    ydb/core/protos
)

END()
