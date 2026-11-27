UNITTEST()

SIZE(SMALL)

SRCS(
    quoter_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/events
    ydb/core/persqueue/public/write_sessions_quoter
    ydb/core/testlib/actors
)

END()
