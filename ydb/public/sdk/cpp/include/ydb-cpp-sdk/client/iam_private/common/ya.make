LIBRARY(client-iam-private-common-include)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    types.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam/common
)

END()
