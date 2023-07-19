LIBRARY()

SRCS(
    portion_info.cpp
    column_record.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme
)

GENERATE_ENUM_SERIALIZATION(portion_info.h)

END()
