PROTO_LIBRARY()

PY_NAMESPACE(yandex.cloud.priv.oauth.v1)

GRPC()
SRCS(
    session_service.proto
    cloud_user.proto
)

PEERDIR(
    ydb/public/api/client/yc_private/iam/v1
    ydb/public/api/client/yc_private/oauth
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

