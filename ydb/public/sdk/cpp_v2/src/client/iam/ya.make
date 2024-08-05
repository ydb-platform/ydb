LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp_v2/src/client/iam/impl
    ydb/public/sdk/cpp_v2/src/client/iam/common
)

END()
