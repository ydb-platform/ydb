LIBRARY()

SRCS(
    GLOBAL session.cpp
    cursor.cpp
    GLOBAL task.cpp
    GLOBAL control.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/session/selector
    ydb/core/tx/columnshard/export/session/storage
    ydb/core/tx/columnshard/bg_tasks
    ydb/core/scheme
    ydb/core/tx/columnshard/export/protos
    ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
