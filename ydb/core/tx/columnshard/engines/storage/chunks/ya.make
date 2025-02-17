LIBRARY()

SRCS(
    data.cpp
    column.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/splitter/abstract
    ydb/core/tx/columnshard/splitter
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/counters
)

END()
