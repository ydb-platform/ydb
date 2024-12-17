LIBRARY()

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
)

SRCS(
    grpc_services/scripting.cpp
    grpc_services/view.cpp
)

END()
