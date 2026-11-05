PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    cluster_workload.proto
    restore_part.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()
