LIBRARY()

SRCS(
    balancing_policies.cpp
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/exceptions
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/issue
    library/cpp/uri
    library/cpp/cgiparam
)

END()
