LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/client/iam/impl
    ydb/public/sdk/cpp/client/iam/common
)

END()
