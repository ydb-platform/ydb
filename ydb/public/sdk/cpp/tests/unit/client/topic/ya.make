UNITTEST()

SIZE(SMALL)
FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic
)

SRCS(
    codecs_ut.cpp
)

END()
