UNITTEST_FOR(ydb/library/actors/testlib)

FORK_SUBTESTS()
SIZE(SMALL)


PEERDIR(
    ydb/library/actors/core
)

SRCS(
    decorator_ut.cpp
    mailbox_processing_finished_ut.cpp
)

END()
