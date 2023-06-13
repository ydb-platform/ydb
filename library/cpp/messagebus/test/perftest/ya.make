PROGRAM(messagebus_perftest)

PEERDIR(
    library/cpp/deprecated/threadable
    library/cpp/execprofile
    library/cpp/getopt
    library/cpp/lwtrace
    library/cpp/messagebus
    library/cpp/messagebus/oldmodule
    library/cpp/messagebus/protobuf
    library/cpp/messagebus/www
    library/cpp/sighandler
    library/cpp/threading/future
)

SRCS(
    messages.proto
    perftest.cpp
    simple_proto.cpp
)

END()
