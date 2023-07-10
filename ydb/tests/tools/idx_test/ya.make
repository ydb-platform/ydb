PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/resource
    ydb/public/lib/idx_test
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

RESOURCE(
    ./sql/create_table1.sql create_table1
)

END()
