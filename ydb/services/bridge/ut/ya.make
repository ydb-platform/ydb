UNITTEST_FOR(ydb/services/bridge)

SIZE(MEDIUM)

SRCS(
    bridge_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/bridge
)

YQL_LAST_ABI_VERSION()

END()
