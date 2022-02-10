PROGRAM(messagebus_rain_check_perftest)

OWNER(g:messagebus)

PEERDIR(
    library/cpp/messagebus/rain_check/core 
    library/cpp/messagebus/rain_check/test/helper 
)

SRCS(
    perftest.cpp
)

END()
