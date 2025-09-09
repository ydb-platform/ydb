LIBRARY()

SRCS(
    balancing_policies.cpp
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/issue
)

END()
