PROGRAM()

SRCS(
    flowctlmain.cpp
)

PEERDIR(
    ydb/library/drr
    library/cpp/lwtrace/mon
    ydb/library/shop
    library/cpp/getopt
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
)

END()
