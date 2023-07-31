LIBRARY()

SRCS(
    address.cpp
    checker.cpp
    merge.cpp
    range.cpp
    reader.cpp
    sources.cpp
    split.cpp
    stopwatch.cpp
    writer.cpp
    util_helpers.cpp
)

PEERDIR(
    library/cpp/getopt/small
    library/cpp/json
    library/cpp/geobase
    library/cpp/int128
)

GENERATE_ENUM_SERIALIZATION(address.h)
GENERATE_ENUM_SERIALIZATION(sources.h)

END()
