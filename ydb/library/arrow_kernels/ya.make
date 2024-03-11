RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
)

GENERATE_ENUM_SERIALIZATION(operations.h)

SRCS(
    func_cast.cpp
    ut_common.cpp
)

END()
