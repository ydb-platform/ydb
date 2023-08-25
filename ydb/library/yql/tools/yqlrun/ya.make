PROGRAM(yqlrun)

ALLOCATOR(J)

SRCS(
    yqlrun.cpp
    gateway_spec.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/getopt
    library/cpp/yson
    ydb/library/yql/sql/pg
    ydb/library/yql/core/facade
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/core/file_storage/http_download
    ydb/library/yql/core/services/mounts
    ydb/library/yql/minikql/comp_nodes/llvm
    ydb/library/yql/protos
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/utils/backtrace
    ydb/library/yql/core
    ydb/library/yql/sql/v1/format
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/core/url_preprocessing
    ydb/library/yql/tools/yqlrun/http
    ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
