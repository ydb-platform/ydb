PROTO_LIBRARY()

    PEERDIR(
        contrib/libs/opentelemetry-proto
    )

    EXCLUDE_TAGS(
        GO_PROTO
        JAVA_PROTO
    )

END()
