LIBRARY()

OWNER(g:kikimr)

SRCS(
    out.cpp
    out_sequenceshard.cpp
)

PEERDIR(
    ydb/core/protos
)

END()
