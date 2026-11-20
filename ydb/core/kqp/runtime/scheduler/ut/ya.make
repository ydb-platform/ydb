UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

SRCS(
    kqp_compute_scheduler_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/kqp/runtime/scheduler
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
