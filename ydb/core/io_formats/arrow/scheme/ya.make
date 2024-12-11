LIBRARY()

SRCS(
    scheme.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/io_formats/arrow/csv_arrow
    ydb/core/scheme_types
)

YQL_LAST_ABI_VERSION()

END()
