LIBRARY()

OWNER(
    g:kikimr
)

SRCS(
    events_scheduling.cpp
)

PEERDIR(
    ydb/library/actors/core
)

END()
