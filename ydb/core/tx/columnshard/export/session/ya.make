LIBRARY()

SRCS(
    session.cpp
    cursor.cpp
    task.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/session/selector
    ydb/core/tx/columnshard/export/session/storage
    ydb/core/scheme
    ydb/core/tx/columnshard/export/protos
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/export/transactions
)

GENERATE_ENUM_SERIALIZATION(session.h)

END()
