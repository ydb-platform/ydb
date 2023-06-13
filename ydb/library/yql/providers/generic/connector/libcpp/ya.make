LIBRARY()

SRCS(
    client_grpc.cpp
    client_mock.cpp
    error.cpp
    utils.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/grpc
    ydb/core/formats/arrow/serializer
    ydb/library/yql/ast
    ydb/library/yql/public/issue
    ydb/library/yql/utils
    ydb/library/yql/providers/generic/connector/api
)

END()

RECURSE(
    cli
)
