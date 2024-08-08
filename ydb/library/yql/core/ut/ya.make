UNITTEST_FOR(ydb/library/yql/core)

SRCS(
    yql_csv_ut.cpp
    yql_column_order_ut.cpp
    yql_execution_ut.cpp
    yql_expr_constraint_ut.cpp
    yql_expr_discover_ut.cpp
    yql_expr_optimize_ut.cpp
    yql_expr_providers_ut.cpp
    yql_expr_type_annotation_ut.cpp
    yql_library_compiler_ut.cpp
    yql_opt_utils_ut.cpp
    yql_udf_index_ut.cpp
    yql_qplayer_ut.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/core/facade
    ydb/library/yql/core/services
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/qplayer/storage/memory
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/core/type_ann
    ydb/library/yql/core/ut_common
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/parser
    ydb/library/yql/providers/result/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/sql/pg
    ydb/library/yql/udfs/common/string
)

RESOURCE(
    ydb/library/yql/cfg/tests/fs.conf fs.conf
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
