PROGRAM(workload_topic_balancing_autopart)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/topic
)

END()
