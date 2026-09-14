PROGRAM(workload_topic_balancing)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/topic
)

END()

RECURSE_FOR_TESTS(
    tests
)
