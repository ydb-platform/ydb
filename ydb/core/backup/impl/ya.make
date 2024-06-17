LIBRARY()

SRCS(
    local_partition_reader.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/tx/replication/service
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut_local_partition_reader
)
