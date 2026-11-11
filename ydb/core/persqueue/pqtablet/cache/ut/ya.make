UNITTEST_FOR(ydb/core/persqueue/pqtablet/cache)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

SRCS(
    cache_eviction_ut.cpp
    pq_l2_cache_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

END()
