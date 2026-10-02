LIBRARY()

SRCS(
    table.cpp
)

PEERDIR(
    ydb/library/formats/arrow/csv/converter
    ydb/public/sdk/cpp/src/client/table
    ydb/public/lib/scheme_types
)

END()
