UNITTEST_FOR(ydb/library/yql/providers/pq/async_io)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    dq_pq_rd_read_actor_ut.cpp
    dq_pq_read_actor_ut.cpp
    dq_pq_write_actor_ut.cpp
)

PEERDIR(
    ydb/core/testlib/basics/default
    ydb/library/testlib/pq_helpers
    ydb/library/yql/providers/common/ut_helpers
    ydb/library/yql/providers/pq/gateway/native
    ydb/public/sdk/cpp/src/client/datastreams
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/tests/fq/pq_async_io
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation/llvm16
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
)

YQL_LAST_ABI_VERSION()

END()
