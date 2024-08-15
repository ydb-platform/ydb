PROGRAM(persqueue_reader_eventloop)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/getopt
)

END()
