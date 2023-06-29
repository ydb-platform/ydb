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
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/api/service
    ydb/library/yql/public/issue
    ydb/library/yql/utils
)

END()

RECURSE(
    cli
)
