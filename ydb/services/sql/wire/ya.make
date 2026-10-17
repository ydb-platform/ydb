LIBRARY()

SRCS(
    connection.cpp
    listener.cpp
)

PEERDIR(
    ydb/core/raw_socket
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/services
)

END()

RECURSE_FOR_TESTS(
    ut
)
