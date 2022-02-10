LIBRARY(messagebus_protobuf)

OWNER(g:messagebus)

SRCS(
    ybusbuf.cpp
)

PEERDIR(
    contrib/libs/protobuf 
    library/cpp/messagebus
    library/cpp/messagebus/actor
)

END()
