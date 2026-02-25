UNITTEST_FOR(ydb/library/yql/providers/pq/gateway)

SIZE(MEDIUM)

SRCS(
    composite_client_ut.cpp
)

PEERDIR(
    ydb/core/testlib/basics
    ydb/library/testlib/common
    ydb/library/testlib/pq_helpers
    ydb/library/yql/providers/pq/async_io
    ydb/library/yql/providers/pq/gateway/clients/composite
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
