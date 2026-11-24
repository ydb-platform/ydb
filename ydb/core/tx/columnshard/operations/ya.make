LIBRARY()

SRCS(
    write.cpp
    write_data.cpp
    manager.cpp
    events.cpp
    write_gate.cpp
    write_task.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/core/tx/data_events
    ydb/services/metadata
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/overload_manager
    ydb/core/tx/columnshard/tracing
    ydb/core/tx/columnshard/data_reader
    ydb/core/tx/columnshard/transactions/locks
    ydb/core/tx/columnshard/operations/batch_builder
    ydb/core/tx/columnshard/operations/slice_builder
    ydb/core/tx/columnshard/operations/common
    library/cpp/lwtrace
)

END()
