PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

PY_NAMESPACE(yandex.cloud.priv.operation)

GRPC()
SRCS(
    operation.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

