PROTO_LIBRARY()

PY_NAMESPACE(yandex.cloud.priv.quota)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    quota.proto
    quota_limit.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/code
    rpc/errdetails
    rpc/status
    type/timeofday
    type/dayofweek
)

END()

