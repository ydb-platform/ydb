LIBRARY()

# gn: move into parent

SRCS(
    controller.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
