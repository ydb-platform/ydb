LIBRARY()

SRCS(
    csv_ydb_dump.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/core/io_formats/cell_maker
)

YQL_LAST_ABI_VERSION()

END()
