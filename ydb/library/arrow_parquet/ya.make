LIBRARY()

SRCS(
    result_set_parquet_printer.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_value
    contrib/libs/apache/arrow
)

END()
