PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

INCLUDE_TAGS(GO_PROTO)

SRCS(
    extension.proto
    yamr.proto
)

END()
