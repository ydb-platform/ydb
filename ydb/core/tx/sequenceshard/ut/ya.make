UNITTEST_FOR(ydb/core/tx/sequenceshard)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    ut_helpers.cpp
    ut_sequenceshard.cpp
)

YQL_LAST_ABI_VERSION()

END()
