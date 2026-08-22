LIBRARY()

SRCS(
    cache_policy.cpp
    manager.cpp
)

PEERDIR(
    ydb/core/tx/general_cache
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/resource_subscriber
)

END()
