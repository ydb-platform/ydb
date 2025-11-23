LIBRARY()

SRCS(
    GLOBAL session.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
    import_actor.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/backup/import/protos
    ydb/core/tx/columnshard/bg_tasks
    ydb/core/tx/columnshard/export/session/selector
    ydb/core/tx/columnshard/export/session/storage
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
