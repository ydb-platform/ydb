PROGRAM()

SRCS(
    main.cpp
    ttl.h
    ttl.cpp
    util.h
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/client/ydb_table
)

END()
