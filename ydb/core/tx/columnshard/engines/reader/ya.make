LIBRARY()

SRCS(
    batch.cpp
    common.cpp
    conveyor_task.cpp
    description.cpp
    filling_context.cpp
    filter_assembler.cpp
    granule.cpp
    processing_context.cpp
    postfilter_assembler.cpp
    queue.cpp
    read_filter_merger.cpp
    read_metadata.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/program
    ydb/core/tx/columnshard/engines/reader/order_control
)

GENERATE_ENUM_SERIALIZATION(read_metadata.h)
YQL_LAST_ABI_VERSION()

END()
