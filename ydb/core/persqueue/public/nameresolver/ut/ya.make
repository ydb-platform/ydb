GTEST()

SIZE(SMALL)

SRCS(
    nameresolver_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/public/nameresolver
    ydb/core/protos
    ydb/library/actors/core
)

END()
