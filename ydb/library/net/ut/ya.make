GTEST()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/library/net
)

SRCS(
    source_address_ut.cpp
)

END()
