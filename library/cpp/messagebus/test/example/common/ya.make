LIBRARY(messagebus_test_example_common)

PEERDIR(
    library/cpp/messagebus
    library/cpp/messagebus/protobuf
)

SRCS(
    proto.cpp
    messages.proto
)

END()
