LIBRARY()

SRC(yql_yt_dq_task_preprocessor.cpp)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/utils/failure_injector
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/codec
    ydb/library/yql/providers/dq/interface
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/gateway/lib
    yt/yql/providers/yt/lib/yson_helpers
)

YQL_LAST_ABI_VERSION()

END()
