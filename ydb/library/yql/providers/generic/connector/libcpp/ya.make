LIBRARY()

SRCS(
    client.cpp
    error.cpp
    utils.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/grpc
    ydb/core/formats/arrow/serializer
    ydb/library/grpc/client
    ydb/library/yql/ast
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/api/service
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/public/issue
    ydb/library/yql/utils
    ydb/library/yql/utils/log
)

END()

RECURSE(
    ut_helpers
)
