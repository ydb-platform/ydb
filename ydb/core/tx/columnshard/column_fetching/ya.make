LIBRARY()

SRCS(
    cache_policy.cpp
)

PEERDIR(
    ydb/core/tx/general_cache
    ydb/core/tx/columnshard
)

END()
