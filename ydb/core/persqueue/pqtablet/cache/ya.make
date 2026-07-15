LIBRARY()

SRCS(
    pq_l2_cache.cpp
)



PEERDIR(
    ydb/core/keyvalue
    ydb/core/persqueue/pqtablet/blob
    ydb/core/persqueue/events
    ydb/public/api/grpc/draft
)

END()

RECURSE_FOR_TESTS(
    ut
)
