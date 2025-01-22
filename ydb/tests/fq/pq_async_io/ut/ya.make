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
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/minikql/computation/llvm14
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/providers/common/ut_helpers
    ydb/library/yql/providers/pq/gateway/native
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    ydb/public/sdk/cpp/src/client/datastreams
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/tests/fq/pq_async_io
)

YQL_LAST_ABI_VERSION()

END()
