PROGRAM(persqueue_reader_eventloop)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/topic
    library/cpp/getopt
)

END()
