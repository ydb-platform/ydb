LIBRARY()

# gn: move into parent

SRCS(
    event.cpp
)

PEERDIR(
)

GENERATE_ENUM_SERIALIZATION(event.h)

END()
