PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    unified_agent.proto
)

GENERATE_ENUM_SERIALIZATION(unified_agent.pb.h)

END()
