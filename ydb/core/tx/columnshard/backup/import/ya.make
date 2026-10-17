LIBRARY()

SRCS(
    GLOBAL session.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL task.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL control.cpp  # gn: into ydb/core/tx/columnshard
    import_actor.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/kqp/compute_actor
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/backup/import/protos
    ydb/core/tx/columnshard/bg_tasks
    ydb/library/signals
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()

RECURSE_FOR_TESTS(
    ut
)
