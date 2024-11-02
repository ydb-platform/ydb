PROGRAM(pgrun)

ALLOCATOR(J)

SRCS(
    pgrun.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/getopt
    library/cpp/string_utils/base64
    contrib/libs/fmt
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/http_download
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
    yql/essentials/protos
    contrib/ydb/library/yql/public/udf/service/exception_policy
    yql/essentials/utils/backtrace
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/lib/schema
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/providers/yt/comp_nodes/llvm14
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/providers/pg/provider
    contrib/ydb/library/yql/core/url_preprocessing
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
