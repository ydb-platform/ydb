LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
    client_pid.cpp
)

PEERDIR(
    ydb/library/grpc/client
    yql/essentials/public/issue
)

END()
