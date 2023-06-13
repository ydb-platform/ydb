LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
)

PEERDIR(
    library/cpp/grpc/client
    ydb/library/yql/public/issue
)

END()
