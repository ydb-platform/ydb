LIBRARY()

SRCS(
    fetched_data.cpp
    columns_set.cpp
    iterator.cpp
    context.cpp
    source.cpp
    fetching.cpp
    fetch_steps.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
