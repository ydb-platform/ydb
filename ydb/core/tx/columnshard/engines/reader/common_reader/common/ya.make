LIBRARY()

SRCS(
    accessor_callback.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    script.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    script_cursor.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    script_counters.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    accessors_ordering.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    columns_set.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
