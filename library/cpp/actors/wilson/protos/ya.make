PROTO_LIBRARY()

    GRPC()

    SRCS(
        common.proto
        resource.proto
        service.proto
        trace.proto
    )

    EXCLUDE_TAGS(
        GO_PROTO
        JAVA_PROTO
    )

END()
