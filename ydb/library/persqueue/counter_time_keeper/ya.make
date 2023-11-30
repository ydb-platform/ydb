LIBRARY()

PEERDIR(
    library/cpp/lwtrace/protos
    ydb/library/actors/protos
    ydb/core/protos
)

SRCS(
    counter_time_keeper.h
    counter_time_keeper.cpp
)

END()
