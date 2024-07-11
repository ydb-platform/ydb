LIBRARY()

SRCS(
    scheme.cpp
    counters.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/versions
)

END()
