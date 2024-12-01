LIBRARY()

SRCS(
    fetched_data.cpp
    columns_set.cpp
)

PEERDIR(
)

GENERATE_ENUM_SERIALIZATION(columns_set.h)

END()
