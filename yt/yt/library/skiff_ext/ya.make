LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)


SRCS(
    serialize.cpp
    schema_match.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/skiff
)

END()
