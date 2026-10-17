LIBRARY()

SRCS(
    context.cpp  # gn: into ydb/core/tx/columnshard/engines
)

PEERDIR(
    ydb/core/tx/columnshard/engines/changes/abstract
)

END()
