PROGRAM(read_from_topic_in_transaction)

SRCS(
    application.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/getopt
)

END()
