LIBRARY()

SRCS(
    schema.cpp
    filtered_schema.cpp
    versioned_index.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/abstract
)

END()
