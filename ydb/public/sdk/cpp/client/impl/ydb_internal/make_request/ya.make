LIBRARY()

OWNER(g:kikimr) 

SRCS(
    make.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos 
)

END()
