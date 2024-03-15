PROTO_LIBRARY()

PY_NAMESPACE(yandex.cloud.priv.oauth.v1)

GRPC()
SRCS(
    session_service.proto
    cloud_user.proto
    claims.proto
)

PEERDIR(
    ydb/public/api/client/yc_private/iam
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

