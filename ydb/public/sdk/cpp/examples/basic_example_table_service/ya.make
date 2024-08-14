PROGRAM()

SRCS(
    main.cpp
    basic_example_data_ts.cpp
    basic_example_ts.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/client/ydb_table
)

END()
