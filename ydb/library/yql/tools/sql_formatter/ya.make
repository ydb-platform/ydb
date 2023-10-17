PROGRAM()

PEERDIR(
    library/cpp/getopt
    contrib/libs/protobuf
    ydb/library/yql/sql/v1/format
)

SRCS(
    sql_formatter.cpp
)

END()
