UNITTEST_FOR(ydb/library/yql/providers/pq/async_io)

SIZE(MEDIUM)

SRCS(
    dq_pq_info_aggregator_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/core/testlib/basics
    ydb/library/testlib/common
    ydb/library/yql/providers/pq/async_io
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
