YQL_LIBRARY()

SRCS(
    compile_context.cpp
    compile_context.h
    compile_result.cpp
    compile_result.h
    db_key_resolver.cpp  # gn: db_key_resolver
    db_key_resolver.h
    mkql_compile_service.cpp
    yql_expr_minikql.cpp
    yql_expr_minikql.h
)
# gn: db_key_resolver headers db_key_resolver.h

PEERDIR(
    ydb/library/actors/core
    library/cpp/threading/future
    ydb/core/base
    ydb/core/engine
    ydb/core/scheme
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/minikql
    yql/essentials/providers/common/mkql
)

END()

RECURSE_FOR_TESTS(
    ut
)
