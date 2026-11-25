PROTO_LIBRARY()

GRPC()
# Give the private C++ stub its own symbols while retaining the canonical RPC path.
SET_APPEND(CPP_PROTO_OPTS --grpc_cpp_opt=services_namespace=YdbDraft)

SRCS(eds.proto)

PEERDIR(ydb/public/api/grpc/draft/xds/wire)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()

RECURSE(
    codec
    wire
)
