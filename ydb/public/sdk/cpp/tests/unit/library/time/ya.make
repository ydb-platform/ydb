GTEST()

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
SRCS(
    time_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/time
)

END()
