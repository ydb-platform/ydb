LIBRARY()

# gn: move into parent

SRCS(
    identifier.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/protos
    ydb/library/conclusion
    ydb/core/protos
)

END()
