LIBRARY()

SRCS(
    stages.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/tx_reader
)

END()
