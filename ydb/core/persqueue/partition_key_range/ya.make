LIBRARY()

OWNER(
    ilnaz
    g:kikimr 
    g:logbroker
)

SRCS(
    partition_key_range.cpp
)

PEERDIR(
    ydb/core/protos 
    ydb/core/scheme 
)

END()
