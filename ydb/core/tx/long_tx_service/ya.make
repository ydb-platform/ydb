LIBRARY()

SRCS(
    acquire_snapshot_impl.cpp
    commit_impl.cpp
    long_tx_service.cpp
    long_tx_service_impl.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/columnshard
    ydb/core/tx/long_tx_service/public
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    public
    ut
    public/ut
)

RECURSE_FOR_TESTS(
    ut
)
