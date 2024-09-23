UNITTEST_FOR(ydb/library/yql/providers/solomon/async_io)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

SRCS(
    dq_solomon_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
    ydb/core/testlib/basics
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/ut_helpers
)

YQL_LAST_ABI_VERSION()

END()
