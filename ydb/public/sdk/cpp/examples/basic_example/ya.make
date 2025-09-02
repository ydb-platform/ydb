PROGRAM()

SRCS(
    main.cpp
    basic_example_data.cpp
    basic_example.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/driver
)

END()
