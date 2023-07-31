LIBRARY()

PEERDIR(
    library/cpp/geo
)

SRCS(
    geohash.cpp
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(direction.h)

END()
