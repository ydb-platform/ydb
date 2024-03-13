LIBRARY()

SRCS(
    counters.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/counters/common
)

END()
