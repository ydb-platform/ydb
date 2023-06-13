LIBRARY()

SRCS(
    mon_messagebus.cpp
    mon_service_messagebus.cpp
)

PEERDIR(
    library/cpp/messagebus
    library/cpp/messagebus/www
    library/cpp/monlib/dynamic_counters
)

END()
