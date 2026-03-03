GTEST()


SRCS(
    consumer_offset_tracker_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/pqtablet/partition
)

REQUIREMENTS(cpu:1)
END()
