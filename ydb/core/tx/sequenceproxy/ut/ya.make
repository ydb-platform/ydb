UNITTEST_FOR(ydb/core/tx/sequenceproxy)

SRCS(
    sequenceproxy_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(cpu:1)
END()
