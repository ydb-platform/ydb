PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    library/cpp/monlib/encode/legacy_protobuf/protos
)

SRCS(
    shareplanner_sensors.proto
    shareplanner.proto
    shareplanner_history.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
