LIBRARY()

SRCS(
    table.cpp
)

PEERDIR(
    ydb/library/formats/arrow/csv/converter
    ydb/public/sdk/cpp/src/client/table
)

END()
