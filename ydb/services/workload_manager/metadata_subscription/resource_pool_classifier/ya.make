LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
    checker.cpp
    fetcher.cpp
    initializer.cpp
    manager.cpp
    object.cpp
    snapshot.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/base
    ydb/core/cms/console/events
    ydb/core/protos
    ydb/core/resource_pools
    ydb/library/query_actor
    ydb/services/metadata
    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/manager
    ydb/services/workload_manager/actors
    ydb/services/workload_manager/common
)

YQL_LAST_ABI_VERSION()

END()
