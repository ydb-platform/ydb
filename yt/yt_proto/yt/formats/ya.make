PROTO_LIBRARY()

IF (JAVA_PROTO)
    DEFAULT_JDK_VERSION(11)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

INCLUDE_TAGS(
    DOCS_PROTO
    GO_PROTO
)

SRCS(
    extension.proto
    yamr.proto
)

PEERDIR_FROM_POLICY(
    ALLOW
    "library/.*/(n)?yt"
    library/cpp/dolbilo/proto_yt
)

END()
