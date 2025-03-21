UNITTEST_FOR(ydb/library/yql/providers/solomon/actors)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

SRCS(
    dq_solomon_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
    ydb/core/testlib/basics
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/providers/common/ut_helpers
)

YQL_LAST_ABI_VERSION()

END()
