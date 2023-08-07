PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

IF (NOT PY_PROTOS_FOR)
    INCLUDE_TAGS(GO_PROTO)
ENDIF()

SRCS(
    extension.proto
    yamr.proto
)

END()
