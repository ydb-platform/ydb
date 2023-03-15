LIBRARY()

PEERDIR(
    contrib/libs/cctz/tzdata
    util/draft
)

SRCS(
    convert.cpp
    civil.cpp
)

GENERATE_ENUM_SERIALIZATION(civil.h)

END()

RECURSE_FOR_TESTS(ut)
