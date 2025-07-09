PROTO_LIBRARY()

IF (OPENSOURCE)
    EXCLUDE_TAGS(GO_PROTO)
ELSE()
    INCLUDE_TAGS(GO_PROTO)
ENDIF()

GRPC()

SRCS(
    data_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/status
)

END()
