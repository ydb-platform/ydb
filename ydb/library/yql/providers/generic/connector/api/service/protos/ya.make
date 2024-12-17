PROTO_LIBRARY()

PEERDIR(
    ydb/library/yql/providers/common/proto
    ydb/public/api/protos
)

# Because Go is excluded in YDB protofiles
ONLY_TAGS(
    CPP_PROTO
    PY_PROTO
    PY3_PROTO
)

SRCS(
    connector.proto
)

END()
