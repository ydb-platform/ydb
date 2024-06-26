LIBRARY()

SRCS(
    local_rpc.cpp
    local_rpc.h
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services/base
)

YQL_LAST_ABI_VERSION()

END()
