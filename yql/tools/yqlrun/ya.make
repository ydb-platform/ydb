PROGRAM(yqlrun)

ALLOCATOR(J)

SRCS(
    yqlrun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    yql/tools/yqlrun/http
    yql/tools/yqlrun/lib

    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/codec/codegen

    yql/essentials/providers/common/provider
    yql/essentials/providers/common/udf_resolve
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_catalog
    yql/essentials/core/services/mounts
    yql/essentials/core/facade
    yql/essentials/core/pg_ext
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/proto
    yql/essentials/core
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/minikql
    yql/essentials/protos
    yql/essentials/ast
    yql/essentials/sql
    yql/essentials/sql/pg
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi

    library/cpp/getopt
    library/cpp/logger

    contrib/libs/protobuf
)

YQL_LAST_ABI_VERSION()

FILES(
    ui.sh
    uig.sh
)

END()
