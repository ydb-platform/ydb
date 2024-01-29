PROTO_LIBRARY()

GRPC()
SRCS(
    claim_service.proto
    claims.proto
    cloud_user.proto
    login_history_service.proto
    oauth_request.proto
    session_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/code
    rpc/errdetails
    rpc/status
    type/timeofday
    type/dayofweek
)

PEERDIR(
    ydb/public/api/client/yc_private/common
    ydb/public/api/client/yc_private/iam
)
END()

