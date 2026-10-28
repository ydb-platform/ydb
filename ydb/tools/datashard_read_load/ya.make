PROGRAM(datashard_read_load)

SRCS(
    main.cpp
    workload.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/histogram/hdr
    library/cpp/json/writer
    ydb/public/sdk/cpp/src/client/table
)

END()

RECURSE_FOR_TESTS(
    ut
)
