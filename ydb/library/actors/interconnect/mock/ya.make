LIBRARY()

SRCS(
    ic_mock.cpp  # gn: into ydb/library/actors/interconnect
    ic_mock.h  # gn: into ydb/library/actors/interconnect
)

SUPPRESSIONS(tsan.supp)

PEERDIR(
    ydb/library/actors/interconnect
)

END()
