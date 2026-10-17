LIBRARY()

# gn: move into parent

SRCS(
    event.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/common
)

END()
