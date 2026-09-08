PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    service.proto
)

PEERDIR(
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
