RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
)

SRCS(
    func_cast.cpp
    ut_common.cpp
)

END()
