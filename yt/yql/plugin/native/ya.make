LIBRARY()

SRCS(
    GLOBAL plugin.cpp
    error_helpers.cpp
    progress_merger.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/resource
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    ydb/library/yql/ast
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/core/facade
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/core/file_storage/http_download
    ydb/library/yql/core/progress_merger
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/user_data
    ydb/library/yql/minikql
    ydb/library/yql/protos
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/log
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/solomon/provider
    ydb/library/yql/core
    ydb/library/yql/core/url_preprocessing
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/lib/log
    ydb/library/yql/providers/yt/lib/yt_download
    ydb/library/yql/providers/yt/provider

    yt/yql/plugin
)

YQL_LAST_ABI_VERSION()

END()
