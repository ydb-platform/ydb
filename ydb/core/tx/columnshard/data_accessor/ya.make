LIBRARY()

SRCS(
    request.cpp
    manager.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/data_accessor/abstract
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/tx/columnshard/data_accessor/cache_policy
    ydb/core/tx/columnshard/resource_subscriber
)

END()
