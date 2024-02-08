LIBRARY()

SRCS(
    ic_mock.cpp
    ic_mock.h
)

SUPPRESSIONS(tsan.supp)

PEERDIR(
    ydb/library/actors/interconnect
)

END()
