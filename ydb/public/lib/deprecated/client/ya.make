LIBRARY()

SRCS(
    msgbus_client.cpp
    msgbus_client.h
    msgbus_client_config.h
    msgbus_player.cpp
    msgbus_player.h
    grpc_client.cpp
)

PEERDIR(
    library/cpp/grpc/client
    library/cpp/messagebus
    ydb/public/lib/base
)

END()
