UNITTEST_FOR(yql/essentials/core/extract_predicate)

SRCS(
    extract_predicate_ut.cpp
)

PEERDIR(
    library/cpp/yson
    yql/essentials/core/facade
    yql/essentials/core/services
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/type_ann
    yql/essentials/providers/config
    yql/essentials/providers/pure
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/provider
    yql/essentials/providers/result/provider
    yql/essentials/core/cbo/simple
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql
    yql/essentials/sql/v1
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
