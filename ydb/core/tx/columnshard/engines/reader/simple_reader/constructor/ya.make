LIBRARY()

SRCS(
    GLOBAL constructor.cpp  # gn: into ydb/core/tx/columnshard
    read_metadata.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/engines/reader/common_reader/constructor
    ydb/core/kqp/compute_actor
)

END()
