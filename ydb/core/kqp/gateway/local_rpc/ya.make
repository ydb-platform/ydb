LIBRARY()

SRCS(
    helper.cpp
)

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/provider
    ydb/library/ydb_issue
)

YQL_LAST_ABI_VERSION()

END()
