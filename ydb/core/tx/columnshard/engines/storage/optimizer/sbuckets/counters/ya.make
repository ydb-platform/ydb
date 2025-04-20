LIBRARY()

SRCS(
    counters.cpp
)

PEERDIR(
    ydb/library/signals
    ydb/core/tx/columnshard/engines/portions
)

END()
