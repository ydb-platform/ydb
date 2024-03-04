LIBRARY()

SRCS(
    controller.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/engines/changes
)

END()
