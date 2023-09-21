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
    committed_assembler.cpp
    columns_set.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/blobs_action
)

END()
