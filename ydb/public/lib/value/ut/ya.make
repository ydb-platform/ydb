GTEST()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/public/lib/value
)

SRCS(
    value_ut.cpp
)

END()
