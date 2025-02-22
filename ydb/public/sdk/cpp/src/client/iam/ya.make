LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/src/client/iam/common
)

END()
