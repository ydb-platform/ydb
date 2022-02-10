LIBRARY()

OWNER(
    alexnick
    g:kikimr 
    g:logbroker
)

SRCS(
    config.h
    config.cpp
)

PEERDIR(
    ydb/core/protos 
)

END()
