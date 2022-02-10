LIBRARY()

OWNER(g:kikimr)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base 
    ydb/core/protos 
)

SRCS(
    blockstore.cpp
)

END()
