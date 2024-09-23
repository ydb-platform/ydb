LIBRARY()

SRCS(
    defs.h
    message_seqno.cpp
    tx.h
    tx.cpp
    tx_processing.h
    tx_proxy_schemereq.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/persqueue/config
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/util
    ydb/library/aclib
)

END()

RECURSE(
    balance_coverage
    columnshard
    coordinator
    datashard
    long_tx_service
    mediator
    replication
    scheme_board
    scheme_cache
    schemeshard
    sequenceproxy
    sequenceshard
    sharding
    tiering
    time_cast
    tracing
    tx_allocator
    tx_allocator_client
    tx_proxy
)
