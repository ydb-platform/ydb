LIBRARY()

OWNER(
    dcherednik
    g:kikimr
)

SRCS(
    accessor.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/lib/operation_id/protos
    ydb/library/yql/public/issue/protos
)

END()
