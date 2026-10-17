LIBRARY()

SRCS(
    index.cpp  # gn: into ydb/core/tx/columnshard/engines
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
