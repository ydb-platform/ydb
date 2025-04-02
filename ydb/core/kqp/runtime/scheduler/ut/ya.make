UNITTEST_FOR(ydb/core/kqp/runtime/scheduler)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics/pg
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
)

SRCS(
    kqp_compute_scheduler_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
