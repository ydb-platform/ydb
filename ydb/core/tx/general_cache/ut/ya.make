UNITTEST()

SRCS(
    ut_request_queue.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/general_cache/service
    ydb/library/actors/core
    ydb/library/signals
    library/cpp/monlib/dynamic_counters
)

END()
