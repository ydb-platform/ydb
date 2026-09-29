LIBRARY(sdk-library-grpc-client-v3)

SRCS(
    bounded_response.cpp
    grpc_client_low.cpp
    grpc_common.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/libs/grpc
    library/cpp/containers/stack_vector
    library/cpp/openssl/holders
    ydb/public/sdk/cpp/src/library/time
)

END()
