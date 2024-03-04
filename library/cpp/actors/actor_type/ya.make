LIBRARY()

SRCS(
    common.cpp
    indexes.cpp
    index_constructor.cpp
)

PEERDIR(
    library/cpp/actors/util
    library/cpp/actors/prof
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
