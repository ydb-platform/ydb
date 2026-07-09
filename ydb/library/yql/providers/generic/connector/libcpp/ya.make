LIBRARY()

SRCS(
    client.cpp
    error.cpp
    utils.cpp
    yt_client.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/grpc
    ydb/core/formats/arrow/serializer
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/library/yql/dq/actors/protos
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/proto
    ydb/library/yql/providers/generic/connector/api/service
    ydb/library/yql/providers/generic/connector/api/service/protos
    yql/essentials/public/issue
    yql/essentials/utils
    yql/essentials/utils/log
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/client
    library/cpp/yson/node
)

END()

RECURSE(
    ut
    ut_helpers
)
