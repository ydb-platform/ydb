PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/public/api/client/nc_private
    ydb/public/api/client/yc_private/accessservice
)

SRCS(
    masking_test.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
