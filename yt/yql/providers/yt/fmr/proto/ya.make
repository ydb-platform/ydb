PROTO_LIBRARY()

SRCS(
    coordinator.proto
    request_options.proto
    table_data_service.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
