UNITTEST_FOR(ydb/services/config)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    bsconfig_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/config
)

YQL_LAST_ABI_VERSION()

END()
