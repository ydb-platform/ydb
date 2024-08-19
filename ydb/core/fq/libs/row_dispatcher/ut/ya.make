UNITTEST_FOR(ydb/core/fq/libs/row_dispatcher)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

SRCS(
    coordinator_ut.cpp
    leader_election_ut.cpp
    json_parser_ut.cpp
    json_filter_ut.cpp
    topic_session_ut.cpp
    row_dispatcher_ut.cpp
)

PEERDIR(
    ydb/core/fq/libs/row_dispatcher
    library/cpp/testing/unittest
    #ydb/core/testlib/actors
    ydb/core/testlib
    ydb/library/yql/udfs/common/json2
    ydb/library/yql/udfs/common/yson2
    ydb/tests/fq/pq_async_io
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
