PROTO_LIBRARY()

SET(PROTOC_TRANSITIVE_HEADERS "no")

GRPC()

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    blobstorage_base3.proto
    entity_status.proto
    group_info.proto
    vdisk_location.proto
)

PEERDIR(
    ydb/library/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
