UNITTEST_FOR(ydb/services/bsconfig)

SIZE(MEDIUM)

SRCS(
    bsconfig_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/bsconfig
)

YQL_LAST_ABI_VERSION()

END()
