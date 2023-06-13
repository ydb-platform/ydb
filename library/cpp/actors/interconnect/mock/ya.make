LIBRARY()

SRCS(
    ic_mock.cpp
    ic_mock.h
)

SUPPRESSIONS(tsan.supp)

PEERDIR(
    library/cpp/actors/interconnect
)

END()
