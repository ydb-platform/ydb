PROGRAM()

SRCS(
    main.cpp
    basic_example_data.cpp
    basic_example.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/client/ydb_query
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
