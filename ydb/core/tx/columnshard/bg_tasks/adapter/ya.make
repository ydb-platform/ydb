LIBRARY()

# gn: move into parent

SRCS(
    adapter.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks/templates
)

END()
