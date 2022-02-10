UNITTEST_FOR(ydb/library/yql/providers/solomon/async_io)

OWNER(
    d-mokhnatkin
    g:yq
    g:yql
)

INCLUDE(${ARCADIA_ROOT}/kikimr/yq/tools/solomon_emulator/recipe/recipe.inc)

SRCS(
    dq_solomon_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
    ydb/core/testlib/basics
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/ut_helpers
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:12) 
 
END()
