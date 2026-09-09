LIBRARY()

SRCS(
    ../interconnect_load.cpp
)

PEERDIR(
    ydb/core/load_test/common
    ydb/library/actors/core
    ydb/library/actors/interconnect
)

END()
