LIBRARY()

SRCS(
    abstract.cpp
    read_metadata.cpp
    constructor.cpp
    read_context.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/insert_table
    ydb/core/tx/program
    ydb/core/protos
    ydb/core/tx/columnshard/data_sharing/protos
)

GENERATE_ENUM_SERIALIZATION(read_metadata.h)

END()
