UNITTEST_FOR(ydb/core/tx/long_tx_service)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    long_tx_service_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
