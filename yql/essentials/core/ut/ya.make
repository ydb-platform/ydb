UNITTEST_FOR(yql/essentials/core)

SRCS(
    yql_column_order_ut.cpp
    yql_default_valid_value_ut.cpp
    yql_expr_constraint_ut.cpp
    yql_expr_optimize_ut.cpp
    yql_library_compiler_ut.cpp
    yql_opt_utils_ut.cpp
    yql_udf_index_ut.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/cbo/simple
    yql/essentials/core/facade
    yql/essentials/core/services
    yql/essentials/core/services/mounts
    yql/essentials/core/file_storage
    yql/essentials/core/qplayer/storage/memory
    yql/essentials/providers/common/udf_resolve
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/type_ann
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/parser
    yql/essentials/providers/pure
    yql/essentials/providers/result/provider
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/udfs/common/string
)

RESOURCE(
    yql/essentials/cfg/tests/fs.conf fs.conf
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
