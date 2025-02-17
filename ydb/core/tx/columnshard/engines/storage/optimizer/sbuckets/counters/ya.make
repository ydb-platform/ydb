LIBRARY()

SRCS(
    counters.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/counters/common
    ydb/core/tx/columnshard/engines/portions
)

END()
