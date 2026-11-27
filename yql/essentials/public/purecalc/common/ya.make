YQL_LIBRARY()

INCLUDE(ya.make.inc)

PEERDIR(
    yt/yql/providers/yt/codec/codegen
    yql/essentials/providers/config
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/pg
)

PEERDIR_FROM_POLICY(
    ALLOW
    logos/projects/abt_bs_calc/tasks/calc_dashboard/alice/calc_kpi_metrics$
    quality/ab_testing/cofe/tests/bin/lib/projects$
    quality/ab_testing/cofe/tests/bin/lib/projects_medium$
    quality/user_sessions/createlib/qb3/parser$
)

END()

RECURSE(
    no_llvm
)
