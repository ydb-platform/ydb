LIBRARY()

GENERATE_ENUM_SERIALIZATION(columns_set.h)

SRCS(
    accessor_callback.cpp
    script.cpp
    script_cursor.cpp
    script_counters.cpp
    accessors_ordering.cpp
    columns_set.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor
)


END()
