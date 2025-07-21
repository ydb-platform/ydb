LIBRARY()

SRCS(
    accessor_callback.cpp
    script.cpp
    script_cursor.cpp
    script_counters.cpp
    columns_set.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
