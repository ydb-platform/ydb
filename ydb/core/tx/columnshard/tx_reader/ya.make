LIBRARY()

SRCS(
    abstract.cpp
    composite.cpp
    lambda.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/counters
)

END()
