LIBRARY()

SRCS(
    builder.cpp
    merger.cpp
    restore.cpp
)

PEERDIR(
    ydb/core/tx/conveyor/usage
    ydb/core/tx/columnshard/common
    ydb/core/tx/data_events
    ydb/core/formats/arrow/reader
    ydb/library/conclusion
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/data_reader
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/data_events/common
    ydb/core/tx/columnshard/engines/scheme
)

END()
