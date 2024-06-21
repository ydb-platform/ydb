LIBRARY()

SRCS(
    tiering.cpp
    counters.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
