UNITTEST_FOR(library/cpp/monlib/dynamic_counters)

OWNER(jamel)

SRCS(
    contention_ut.cpp
    counters_ut.cpp
    encode_ut.cpp
)

PEERDIR(
    library/cpp/monlib/encode/protobuf
    library/cpp/monlib/encode/json
)

END()
