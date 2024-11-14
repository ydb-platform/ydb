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
    yql/essentials/ast
    yql/essentials/sql/pg
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/defs
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/http_download
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/codegen/no_llvm
    yql/essentials/protos
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/udf_resolve
    yql/essentials/core/url_preprocessing
    yql/essentials/core/url_lister
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/lib/log
    ydb/library/yql/providers/yt/lib/yt_download
    ydb/library/yql/providers/yt/lib/yt_url_lister
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
    ydb/library/yql/providers/yt/comp_nodes/no_llvm
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()

