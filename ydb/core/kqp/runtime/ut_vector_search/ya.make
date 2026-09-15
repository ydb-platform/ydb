UNITTEST_FOR(ydb/core/kqp/runtime)

# Kept apart from ydb/core/kqp/runtime/ut, which peers ydb/core/kqp/ut/common (a whole
# test server); these tests need nothing but the actor system.
# MEDIUM rather than SMALL: 46 tests, each spinning up a test actor runtime.
SIZE(MEDIUM)

SRCS(
    kqp_vector_search_actor_ut.cpp
)

# PG support is required (TScopedAlloc's constructor calls PgInitializeMainContext), but a
# stub suffices: ydb/core/testlib/basics/pg would pull the real pg_wrapper, whose PROVIDES
# clashes with pg_dummy's anyway.
PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/actors
    ydb/core/testlib/basics
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
