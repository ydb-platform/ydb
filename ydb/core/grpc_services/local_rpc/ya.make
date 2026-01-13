LIBRARY()

SRCS(
    local_rpc.h
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services/base
    ydb/library/actors/wilson
    ydb/library/wilson_ids
)

YQL_LAST_ABI_VERSION()

END()
