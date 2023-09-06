LIBRARY()

SRCS(
    common.cpp
    conveyor_task.cpp
    description.cpp
    queue.cpp
    read_filter_merger.cpp
    read_metadata.cpp
    read_context.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/resources
    ydb/core/tx/program
    ydb/core/tx/columnshard/engines/reader/plain_reader
    ydb/core/tx/columnshard/engines/scheme
)

GENERATE_ENUM_SERIALIZATION(read_metadata.h)
YQL_LAST_ABI_VERSION()

END()
