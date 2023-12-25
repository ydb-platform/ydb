UNITTEST_FOR(ydb/library/yql/core)

SRCS(
    yql_csv_ut.cpp
    yql_execution_ut.cpp
    yql_expr_constraint_ut.cpp
    yql_expr_discover_ut.cpp
    yql_expr_optimize_ut.cpp
    yql_expr_providers_ut.cpp
    yql_expr_type_annotation_ut.cpp
    yql_library_compiler_ut.cpp
    yql_opt_utils_ut.cpp
    yql_udf_index_ut.cpp
)

PEERDIR(
    library/cpp/yson
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/core/facade
    ydb/library/yql/core/services
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/core/type_ann
    ydb/library/yql/core/ut_common
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/parser
    ydb/library/yql/providers/result/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/minikql/comp_nodes/llvm
    ydb/library/yql/minikql/invoke_builtins/llvm
    ydb/library/yql/sql/pg
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
