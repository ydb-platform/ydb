LIBRARY()

SRCS(
    abstract_scheme.cpp
    snapshot_scheme.cpp
    filtered_scheme.cpp
    versioned_index.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract
)

END()
