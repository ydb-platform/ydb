UNITTEST_FOR(ydb/core/kqp/runtime)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_scan_data_ut.cpp
    kqp_compute_scheduler_ut.cpp
    kqp_scan_fetcher_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics/pg
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/comp_nodes/dq/llvm16
)

END()
