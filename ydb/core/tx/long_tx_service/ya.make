LIBRARY()

SRCS(
    acquire_snapshot_impl.cpp
    commit_impl.cpp
    long_tx_service.cpp
    long_tx_service_impl.cpp
    lwtrace_probes.cpp
    snapshots_exchange.cpp
    snapshots_storage.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages
    library/cpp/random_provider
    library/cpp/time_provider
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/mon
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tx
    ydb/core/tx/columnshard/public
    ydb/core/tx/datashard
    ydb/core/tx/long_tx_service/public
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    ydb/core/util
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    public
    public/ut
    ut
)

RECURSE_FOR_TESTS(
    ut
)
