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
    library/cpp/svnversion
    yql/essentials/sql/pg
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/http_download
    yql/essentials/core/pg_ext
    yql/essentials/core/services/mounts
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/protos
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/utils/backtrace
    yql/essentials/core
    yql/essentials/sql/v1/format
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/udf_resolve
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    yql/essentials/core/url_preprocessing
    ydb/library/yql/tools/yqlrun/http
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/result_format
)

YQL_LAST_ABI_VERSION()

FILES(
    ui.sh
    uig.sh
)

END()
