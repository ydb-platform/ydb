LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
    client_pid.cpp
)

PEERDIR(
    ydb/library/grpc/client
    ydb/library/yql/public/issue
)

END()
