LIBRARY()

SRCS(
    pq_l2_cache.cpp
)



PEERDIR(
    ydb/core/keyvalue
    ydb/core/persqueue/pqtablet/blob
)

END()

RECURSE_FOR_TESTS(
    ut
)

