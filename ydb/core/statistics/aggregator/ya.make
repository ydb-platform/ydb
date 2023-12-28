LIBRARY()

OWNER(
    monster
    g:kikimr
)

SRCS(
    aggregator.h
    aggregator.cpp
    aggregator_impl.h
    aggregator_impl.cpp
    schema.h
    schema.cpp
    tx_configure.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_schemeshard_stats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
)

YQL_LAST_ABI_VERSION()

END()
