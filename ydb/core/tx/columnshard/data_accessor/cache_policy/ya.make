LIBRARY()

SRCS(
    consumers.cpp
    policy.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/blobs_action/counters
    ydb/core/tx/columnshard/data_accessor/abstract
    ydb/core/tx/general_cache/usage
)

END()
