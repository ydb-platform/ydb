LIBRARY()

SRCS(
    scanner.cpp
    source.cpp
    constructor.cpp
    interval.cpp
    fetched_data.cpp
    plain_read_data.cpp
    filter_assembler.cpp
    column_assembler.cpp
)

PEERDIR(
    ydb/core/formats/arrow
)

END()
