GO_LIBRARY()

PEERDIR(ydb/library/yql/providers/generic/connector/libgo/service/protos)

SRCS(
    connector.pb.go
    connector_grpc.pb.go
)

END()

RECURSE(
    protos
)
