LIBRARY()

SRCS(
    compile_context.cpp
    compile_context.h
    compile_result.cpp
    compile_result.h
    db_key_resolver.cpp
    db_key_resolver.h
    mkql_compile_service.cpp
    yql_expr_minikql.cpp
    yql_expr_minikql.h
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/threading/future
    ydb/core/base
    ydb/core/engine
    ydb/core/kqp/provider
    ydb/core/scheme
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/minikql
    ydb/library/yql/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
