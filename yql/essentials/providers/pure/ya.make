LIBRARY()

SRCS(
    yql_pure_provider.cpp
    yql_pure_provider.h
)

PEERDIR(
    yql/essentials/core
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/utils/log
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/transform
    yql/essentials/providers/common/schema/expr
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/mkql_simple_file
    yql/essentials/providers/common/codec
    yql/essentials/providers/result/expr_nodes
    yql/essentials/core/peephole_opt
    yql/essentials/minikql/comp_nodes
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/comp_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
