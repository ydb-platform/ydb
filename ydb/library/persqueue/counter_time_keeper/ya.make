OWNER(g:logbroker) 

LIBRARY()

PEERDIR(
    library/cpp/actors/protos 
    ydb/core/protos 
)

SRCS(
    counter_time_keeper.h
    counter_time_keeper.cpp
)

END()
