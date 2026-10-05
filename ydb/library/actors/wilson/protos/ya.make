PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

    PEERDIR(
        contrib/proto/opentelemetry
    )

    EXCLUDE_TAGS(
        GO_PROTO
        JAVA_PROTO
    )

END()
