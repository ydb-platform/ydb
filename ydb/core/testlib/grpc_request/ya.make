LIBRARY()

SRCS(
    grpc_request.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/grpc_services/base
    ydb/library/ydb_issue
    ydb/public/api/protos
)

END()
