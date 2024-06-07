PROGRAM()

SRCS(
    main.cpp
    vector_index.cpp
    clusterization.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/client/ydb_table
)

END()
