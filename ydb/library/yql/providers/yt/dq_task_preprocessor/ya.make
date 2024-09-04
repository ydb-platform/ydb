LIBRARY()

SRC(yql_yt_dq_task_preprocessor.cpp)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/utils/failure_injector
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/dq/interface
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/gateway/lib
    ydb/library/yql/providers/yt/lib/yson_helpers
)

YQL_LAST_ABI_VERSION()

END()
