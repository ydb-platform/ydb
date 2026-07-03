LIBRARY(sdk-library-grpc-client-v3)

SRCS(
    grpc_client_low.cpp
    grpc_common.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/containers/stack_vector
    library/cpp/openssl/holders
    ydb/public/sdk/cpp/src/library/time
)

END()
