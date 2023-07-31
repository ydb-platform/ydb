LIBRARY()

SRCS(
    library/cpp/geobase/geobase.cpp
)

PEERDIR(
    geobase/library
)

GENERATE_ENUM_SERIALIZATION(geobase/include/structs.hpp)

END()
