PROTO_LIBRARY()

SRCS(
    gateways_config.proto
    udf_resolver.proto
)

PEERDIR(
    ydb/library/yql/protos
    ydb/library/yql/providers/generic/connector/api/common
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()
