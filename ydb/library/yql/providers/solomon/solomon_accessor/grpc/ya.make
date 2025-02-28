PROTO_LIBRARY()

IF (OPENSOURCE)
    EXCLUDE_TAGS(GO_PROTO)
ELSE()
    INCLUDE_TAGS(GO_PROTO)
ENDIF()

GRPC()

SRCS(
    solomon_accessor_pb.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/status
)

END()
