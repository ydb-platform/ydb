LIBRARY()

OWNER(g:messagebus)

PEERDIR(
    library/cpp/messagebus 
    library/cpp/messagebus/actor 
)

SRCS(
    module.cpp
    startsession.cpp
)

END()
