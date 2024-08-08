UNITTEST_FOR(ydb/library/yql/providers/pq/async_io)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    dq_pq_read_actor_ut.cpp
    dq_pq_write_actor_ut.cpp
    ut_helpers.cpp
)

PEERDIR(
    ydb/core/testlib/basics/default
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/ut_helpers
    ydb/library/yql/sql
    ydb/public/sdk/cpp/client/ydb_datastreams
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/library/yql/minikql/comp_nodes/llvm14
)

YQL_LAST_ABI_VERSION()

END()
