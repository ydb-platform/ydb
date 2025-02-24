LIBRARY(sdk-library-grpc-client-v3)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    grpc_client_low.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/containers/stack_vector
)

END()
