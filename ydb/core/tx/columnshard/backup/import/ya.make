LIBRARY()

SRCS(
    GLOBAL session.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
    import_actor.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/core/kqp/common/events
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/backup/import/protos
    ydb/core/tx/columnshard/bg_tasks
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/engines/protos
    ydb/core/tx/columnshard/export/protos
    ydb/library/signals
)

GENERATE_ENUM_SERIALIZATION(import_actor.h)
GENERATE_ENUM_SERIALIZATION(session.h)

END()

RECURSE_FOR_TESTS(
    ut
)
