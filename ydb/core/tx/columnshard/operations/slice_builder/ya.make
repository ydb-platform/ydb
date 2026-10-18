LIBRARY()

# gn: move into parent

SRCS(
    builder.cpp
    pack_builder.cpp
)

PEERDIR(
    ydb/core/tx/conveyor/usage
    ydb/core/tx/data_events
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/columnshard/tracing
    ydb/core/tx/columnshard/hooks/abstract
)
# gn: peerdir ydb/core/tx/columnshard:write_actor
# gn: peerdir ydb/core/tx/columnshard/operations:events

END()
