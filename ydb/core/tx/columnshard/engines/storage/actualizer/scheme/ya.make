LIBRARY()

SRCS(
    scheme.cpp  # gn: into ydb/core/tx/columnshard/engines
    counters.cpp  # gn: into ydb/core/tx/columnshard/engines
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
