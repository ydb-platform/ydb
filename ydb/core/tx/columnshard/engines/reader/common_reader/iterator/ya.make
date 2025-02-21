LIBRARY()

SRCS(
    columns_set.cpp
    constructor.cpp
    context.cpp
    fetch_steps.cpp
    fetched_data.cpp
    fetching.cpp
    iterator.cpp
    source.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme
    yql/essentials/minikql
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
