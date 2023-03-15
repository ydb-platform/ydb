LIBRARY(messagebus_protobuf)

SRCS(
    ybusbuf.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/messagebus
    library/cpp/messagebus/actor
)

END()
