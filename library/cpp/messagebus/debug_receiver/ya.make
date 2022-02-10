PROGRAM(messagebus_debug_receiver) 
 
OWNER(g:messagebus)
 
SRCS( 
    debug_receiver.cpp 
    debug_receiver_proto.cpp 
    debug_receiver_handler.cpp 
) 
 
PEERDIR( 
    library/cpp/getopt
    library/cpp/lwtrace
    library/cpp/messagebus
) 
 
END() 
