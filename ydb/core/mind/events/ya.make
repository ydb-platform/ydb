LIBRARY()

SRCS(
    defs.h
    node_broker.h
    tenant_pool.h
    tenant_slot_broker.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/interconnect
)

END()
