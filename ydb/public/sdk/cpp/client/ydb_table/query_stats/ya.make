LIBRARY()

OWNER(
    dcherednik
    g:kikimr
)

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
)

END()
