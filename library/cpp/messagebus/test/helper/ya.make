LIBRARY(messagebus_test_helper)

SRCS(
    example.cpp
    example_module.cpp
    fixed_port.cpp
    message_handler_error.cpp
    hanging_server.cpp
)

PEERDIR(
    library/cpp/messagebus/oldmodule
    library/cpp/deprecated/atomic
)

END()
