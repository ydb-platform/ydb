LIBRARY()

SRCS(
    iam_grpc_mock_server.cpp
    iam_http_mock_server.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/common
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/src/client/iam
)

END()
