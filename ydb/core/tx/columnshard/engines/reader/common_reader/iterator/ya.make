LIBRARY()

SRCS(
    GLOBAL dictionary_fetching.cpp
    GLOBAL sub_columns_fetching.cpp
    GLOBAL default_fetching.cpp
    constructor.cpp
    context.cpp
    fetch_steps.cpp
    fetched_data.cpp
    fetching.cpp
    iterator.cpp
    source.cpp
)

PEERDIR(
    ydb/core/formats/arrow/accessor/dictionary
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/filter
    ydb/core/tx/columnshard/engines/reader/tracing
    ydb/core/tx/columnshard/engines/scheme
    ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    ydb/core/tx/columnshard/engines/reader/common
    ydb/core/tx/limiter/grouped_memory/usage
    ydb/core/util/evlog
    yql/essentials/minikql
)

GENERATE_ENUM_SERIALIZATION(source.h)
YQL_LAST_ABI_VERSION()

END()
