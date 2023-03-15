LIBRARY()

PEERDIR(
    library/cpp/messagebus
    library/cpp/messagebus/rain_check/core
)

SRCS(
    messagebus_client.cpp
    messagebus_server.cpp
)

END()
