LIBRARY()

SRCS(
    table.cpp
)

PEERDIR(
    ydb/core/io_formats/arrow/csv_arrow
    ydb/public/sdk/cpp/client/ydb_table
)

END()
