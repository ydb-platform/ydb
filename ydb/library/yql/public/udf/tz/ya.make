LIBRARY()

SRCS(
    udf_tz.cpp
    udf_tz.h
)

PEERDIR(
)

END()

RECURSE_FOR_TESTS(
    ut
)
