LIBRARY()

SRCS(
    pq_database.cpp
    pq_rl_helpers.cpp
    utils.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
)

END()
