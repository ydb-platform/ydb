UNITTEST_FOR(ydb/core/kqp/runtime)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_scan_data_ut.cpp
)

IF (USE_OLD_SCHEDULER)
    SRCS(
        scheduler/old/kqp_compute_scheduler_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics/pg
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
)

END()
