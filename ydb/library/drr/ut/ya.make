UNITTEST()

REQUIREMENTS(cpu:1)
PEERDIR(
    library/cpp/threading/future
    ydb/library/drr
)

SRCS(
    drr_ut.cpp
)

END()
