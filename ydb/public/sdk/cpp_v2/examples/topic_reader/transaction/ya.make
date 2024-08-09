PROGRAM(read_from_topic_in_transaction)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    application.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/topic
    library/cpp/getopt
)

END()
