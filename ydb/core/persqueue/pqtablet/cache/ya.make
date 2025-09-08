LIBRARY()

SRCS(
    pq_l2_cache.cpp
)



PEERDIR(
    ydb/core/base
    ydb/core/keyvalue
    ydb/core/persqueue/pqtablet/blob
    ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
    ut
)

