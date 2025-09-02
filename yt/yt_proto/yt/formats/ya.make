PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

INCLUDE_TAGS(
    DOCS_PROTO
    GO_PROTO
)

SRCS(
    extension.proto
    yamr.proto
)

END()
