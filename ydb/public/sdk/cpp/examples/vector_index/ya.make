PROGRAM()

SRCS(
    main.cpp
    vector_index.cpp
    clusterizer.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/dot_product
    ydb/public/sdk/cpp/client/ydb_table
)

END()
