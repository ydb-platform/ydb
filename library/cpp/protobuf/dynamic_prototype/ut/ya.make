UNITTEST()

SRCS(
    dynamic_prototype_ut.cpp
    my_inner_message.proto
    my_message.proto
)

PEERDIR(
    library/cpp/protobuf/dynamic_prototype
)

END()
