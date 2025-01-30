LIBRARY()

SRCS(
    acquire_snapshot_impl.cpp
    commit_impl.cpp
    long_tx_service.cpp
    long_tx_service_impl.cpp
    lwtrace_probes.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    ydb/core/base
    ydb/core/tx/columnshard
    ydb/core/tx/long_tx_service/public
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
