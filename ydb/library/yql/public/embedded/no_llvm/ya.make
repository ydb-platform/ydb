LIBRARY()

SRCDIR(
    ydb/library/yql/public/embedded
)

ADDINCL(
    ydb/library/yql/public/embedded
)

SRCS(
    yql_embedded.cpp
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
    ydb/library/yql/core/facade
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/file_storage/defs
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/core/file_storage/http_download
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/user_data
    ydb/library/yql/minikql
    ydb/library/yql/minikql/invoke_builtins/no_llvm
    ydb/library/yql/minikql/comp_nodes/no_llvm
    ydb/library/yql/minikql/computation/no_llvm
    ydb/library/yql/minikql/codegen/no_llvm
    ydb/library/yql/protos
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/log
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/core/url_preprocessing
    ydb/library/yql/core/url_lister
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/lib/log
    ydb/library/yql/providers/yt/lib/yt_download
    ydb/library/yql/providers/yt/lib/yt_url_lister
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
    ydb/library/yql/providers/yt/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

END()

