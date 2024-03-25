LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/columnshard/export/common
    ydb/core/tx/columnshard/export/session
)

END()
