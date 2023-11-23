PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/threading/future
    ydb/public/sdk/cpp/client/ydb_persqueue_public
)

END()

RECURSE_FOR_TESTS(
    test
)
