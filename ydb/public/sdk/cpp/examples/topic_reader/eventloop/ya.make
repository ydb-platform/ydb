PROGRAM(persqueue_reader_eventloop)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/getopt
)

END()
