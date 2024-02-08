LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
)

PEERDIR(
    ydb/library/grpc/client
    ydb/library/yql/public/issue
)

END()
