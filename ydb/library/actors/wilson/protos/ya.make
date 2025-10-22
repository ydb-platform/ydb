PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

    PEERDIR(
        contrib/libs/opentelemetry-proto
    )

    EXCLUDE_TAGS(
        GO_PROTO
        JAVA_PROTO
    )

END()
