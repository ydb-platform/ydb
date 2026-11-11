LIBRARY()

SRCS(
    move_data.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/blobs_action/storages_manager
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/resource_subscriber
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
