LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
)

SRCS(
    iterator.cpp
    metadata.cpp
    granule_view.cpp
    filler.cpp
    policy.cpp
)

END()

