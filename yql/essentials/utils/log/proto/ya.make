PROTO_LIBRARY()

SRCS(
    logger_config.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

PEERDIR_FROM_POLICY(
    ALLOW
    quality/relev_tools/personalization/user_history/antifraud/udf/library
)

END()
