LIBRARY()

SRCS(
    builder.cpp
)

PEERDIR(
    ydb/core/tx/conveyor/usage
    ydb/core/tx/data_events
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/writer
)

END()
