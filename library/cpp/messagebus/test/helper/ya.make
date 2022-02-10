LIBRARY(messagebus_test_helper)

OWNER(g:messagebus) 

SRCS(
    example.cpp
    example_module.cpp
    fixed_port.cpp
    message_handler_error.cpp
    hanging_server.cpp
)

PEERDIR(
    library/cpp/messagebus/oldmodule
)

END()
