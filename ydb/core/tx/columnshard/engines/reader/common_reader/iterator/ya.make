LIBRARY()

SRCS(
    fetched_data.cpp
    columns_set.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
