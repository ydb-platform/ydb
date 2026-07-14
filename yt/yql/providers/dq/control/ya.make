LIBRARY()

SRCS(
    yql_dq_control.cpp
)

PEERDIR(
    library/cpp/svnversion
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/core/file_storage
    yql/essentials/minikql
    yql/essentials/providers/common/proto
    yql/essentials/utils/log
    yt/yql/providers/dq/config
)

YQL_LAST_ABI_VERSION()

END()
