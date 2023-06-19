# WARNING: never add dependency to YDB protofiles here,
# because it would break Go code generation.

PROTO_LIBRARY()

SRCS(
    data_source.proto
    endpoint.proto
)

END()
