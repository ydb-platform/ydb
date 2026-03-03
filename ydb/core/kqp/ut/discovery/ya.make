UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/discovery
    ydb/core/kqp/ut/common

    yql/essentials/sql/pg_dummy
)

SRCS(
    kqp_discovery_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
