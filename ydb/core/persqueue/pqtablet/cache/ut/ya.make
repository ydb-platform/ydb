UNITTEST_FOR(ydb/core/persqueue/pqtablet/cache)

SRCS(
    cache_eviction_ut.cpp
    pq_l2_cache_ut.cpp
)

PEERDIR(
    ydb/core/testlib/basics
    ydb/core/testlib/default
)

END()
