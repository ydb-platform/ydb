UNITTEST_FOR(ydb/core/kqp)

SRCS(
    kqp_generic_provider_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
