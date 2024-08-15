LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/yql_common/issue
)

END()
