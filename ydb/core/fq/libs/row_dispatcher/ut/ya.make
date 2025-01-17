UNITTEST_FOR(ydb/core/fq/libs/row_dispatcher)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    coordinator_ut.cpp
    leader_election_ut.cpp
    row_dispatcher_ut.cpp
    topic_session_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/fq/libs/row_dispatcher/format_handler/ut/common
    ydb/core/fq/libs/row_dispatcher
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/tests/fq/pq_async_io
    yql/essentials/sql/pg_dummy
    ydb/library/yql/providers/pq/gateway/dummy
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
