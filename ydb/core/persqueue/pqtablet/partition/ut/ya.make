GTEST()


SRCS(
    consumer_offset_tracker_ut.cpp
    message_id_deduplicator_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/pqtablet/partition
    ydb/core/persqueue/common
    ydb/core/protos
)

END()
