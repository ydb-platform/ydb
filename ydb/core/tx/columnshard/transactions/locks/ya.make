LIBRARY()

SRCS(
    dependencies.cpp
    interaction.cpp
    abstract.cpp
    GLOBAL read_start.cpp
    GLOBAL read_finished.cpp
    GLOBAL write.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/transactions/protos
    ydb/core/tx/columnshard/engines/predicate
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/data_sharing/destination/events
)

END()
