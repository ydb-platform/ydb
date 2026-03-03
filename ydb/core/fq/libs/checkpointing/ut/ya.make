UNITTEST_FOR(ydb/core/fq/libs/checkpointing)

SRCS(
    checkpoint_coordinator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/fq/libs/checkpointing
    ydb/core/testlib/actors
    ydb/core/testlib/basics/default
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

YQL_LAST_ABI_VERSION()

END()
