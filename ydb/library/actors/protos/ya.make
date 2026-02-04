PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    actors.proto
    interconnect.proto
    log.proto
    services_common.proto
    unittests.proto
)

PEERDIR(
    ydb/public/api/protos/annotations
)

EXCLUDE_TAGS(GO_PROTO)

END()
