LIBRARY()

OWNER(g:kikimr)

PEERDIR(
    ydb/library/actors/core
    library/cpp/logger
)

SRCS(
    actor.cpp
)

END()
