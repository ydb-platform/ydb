LIBRARY()

SRCS(
    stages.cpp  # gn: into ydb/core/tx/columnshard/engines
)

PEERDIR(
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/tx_reader
)

END()
