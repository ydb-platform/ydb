UNITTEST_FOR(ydb/core/yq/libs/checkpointing)

OWNER(g:yq) 

SRCS(
    checkpoint_coordinator_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/actors
    ydb/core/testlib/basics
    ydb/core/yq/libs/checkpointing
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END() 
