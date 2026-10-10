UNITTEST_FOR(ydb/core/tx/sequenceproxy)

REQUIREMENTS(cpu:1)
SRCS(
    sequenceproxy_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
