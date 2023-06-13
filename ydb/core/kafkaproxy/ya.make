LIBRARY()

SRCS(
    kafka_messages.cpp
    kafka_messages_int.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    ydb/core/base
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
